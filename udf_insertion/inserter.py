"""Utilities to insert dummy UDF calls into libcst ASTs based on type annotations.

Primary function: `insert_udfs_by_types(module, n)`.

Behavior summary:
- Scans a `libcst.Module` for annotated assignments (`x: T = call(...)`).
- Infers argument types for calls from literals or earlier annotated variables.
- Matches available UDFs in `udf_insertion.dummy_udfs` by (arg types, return type).
- Replaces up to `n` annotated-call assignments with calls to matching UDFs.
- Adds `from udf_insertion.dummy_udfs import <name>` imports for used UDFs.

This is intentionally conservative and heuristic — it prioritizes
simple, statically-annotated patterns commonly produced when converting
SQL expressions into typed Python snippets.
"""
from __future__ import annotations

from typing import Dict, List, Optional, Tuple, Set
import random
import libcst as cst
from libcst import matchers as m


def _get_annotation_str(module: cst.Module, ann: cst.Annotation) -> str:
    # Return source code for an annotation node (normalized)
    return module.code_for_node(ann.annotation).strip()


def _infer_literal_type(node: cst.BaseExpression) -> Optional[str]:
    # Simple literal inference for ints, floats, strs, bytes
    if isinstance(node, cst.Integer):
        return "int"
    if isinstance(node, cst.Float):
        return "float"
    if isinstance(node, cst.SimpleString):
        return "str"
    # datetime/Decimal literals not handled here
    return None


def _collect_var_annotations(module: cst.Module) -> Dict[str, str]:
    # Walk module to collect simple variable annotations from AnnAssign nodes
    var_ann: Dict[str, str] = {}

    class VarAnnVisitor(cst.CSTVisitor):
        def visit_AnnAssign(self, node: cst.AnnAssign) -> None:
            target = node.target
            if isinstance(target, cst.Name) and node.annotation is not None:
                ann_str = _get_annotation_str(module, node.annotation)
                var_ann[target.value] = ann_str

    module.visit(VarAnnVisitor())
    return var_ann


def _build_udf_registry() -> Dict[Tuple[Tuple[str, ...], str], List[Tuple[str, List[str]]]]:
    # Build a simple registry mapping (arg_type_names..., return_type_name) -> [(udf_name, [param_kind_names])]
    import inspect
    import typing

    from udf_insertion import dummy_udfs

    registry: Dict[Tuple[Tuple[str, ...], str], List[Tuple[str, List[str]]]] = {}
    for fn in dummy_udfs.all_simple_functions:
        name = fn.__name__
        try:
            hints = typing.get_type_hints(fn, globals(), locals())
        except Exception:
            # fallback: inspect.signature with annotation strings
            sig = inspect.signature(fn)
            params = []
            for p in sig.parameters.values():
                if p.annotation is inspect._empty:
                    params.append("Any")
                else:
                    params.append(getattr(p.annotation, "__name__", str(p.annotation)))
            ret = sig.return_annotation
            ret_name = getattr(ret, "__name__", str(ret)) if ret is not inspect._empty else "Any"
            key = (tuple(params), ret_name)
            kinds = [p.kind.name for p in sig.parameters.values()]
            registry.setdefault(key, []).append((name, kinds))
            continue

        # Construct param sequence in order
        sig = inspect.signature(fn)
        params = []
        for p in sig.parameters.values():
            ann = hints.get(p.name, None)
            if ann is None:
                params.append("Any")
            else:
                params.append(getattr(ann, "__name__", getattr(ann, "__qualname__", str(ann))))
        ret_ann = hints.get("return", None)
        ret_name = getattr(ret_ann, "__name__", getattr(ret_ann, "__qualname__", str(ret_ann))) if ret_ann is not None else "Any"
        key = (tuple(params), ret_name)
        kinds = [p.kind.name for p in sig.parameters.values()]
        registry.setdefault(key, []).append((name, kinds))

    return registry


def insert_udfs_by_types(module: cst.Module, n: int, seed: Optional[int] = None) -> cst.Module:
    """Insert up to `n` UDF calls into `module` based on type annotations.

    Returns a new `cst.Module` with replacements and import statements added.
    """
    if seed is not None:
        random.seed(seed)

    var_ann = _collect_var_annotations(module)
    registry = _build_udf_registry()

    # Find candidate AnnAssign nodes where value is a Call and target has annotation
    candidates: List[Tuple[str, cst.AnnAssign, List[str], str]] = []

    class CandidateVisitor(cst.CSTVisitor):
        def visit_AnnAssign(self, node: cst.AnnAssign) -> None:
            # Only consider simple name targets with annotation and call values
            if not isinstance(node.target, cst.Name):
                return
            if node.annotation is None:
                return
            if not isinstance(node.value, cst.Call):
                return
            target_name = node.target.value
            ret_type = _get_annotation_str(module, node.annotation)

            # Infer argument types
            arg_types: List[str] = []
            ok = True
            for arg in node.value.args:
                val = arg.value
                lit = _infer_literal_type(val)
                if lit is not None:
                    arg_types.append(lit)
                    continue
                if isinstance(val, cst.Name):
                    t = var_ann.get(val.value)
                    if t is None:
                        ok = False
                        break
                    arg_types.append(t)
                    continue
                # unsupported arg expression
                ok = False
                break

            if not ok:
                return

            candidates.append((target_name, node, arg_types, ret_type))

    module.visit(CandidateVisitor())

    # For each candidate, find matching UDFs
    matches: List[Tuple[str, str]] = []  # (target_var, udf_name)
    for target, node, arg_types, ret_type in candidates:
        key = (tuple(arg_types), ret_type)
        udf_entries = registry.get(key, [])
        if not udf_entries:
            continue
        # filter entries to prefer those callable with positional args
        positional_ok: List[str] = []
        for name, kinds in udf_entries:
            # count positional params
            pos_count = sum(1 for k in kinds if k in ("POSITIONAL_ONLY", "POSITIONAL_OR_KEYWORD"))
            if pos_count == len(arg_types):
                positional_ok.append(name)
        if positional_ok:
            matches.append((target, random.choice(positional_ok)))
        else:
            # fallback to any entry
            matches.append((target, random.choice([n for n, _ in udf_entries])))

    if not matches:
        return module

    # Choose up to n unique targets to replace
    chosen = dict(random.sample(matches, min(n, len(matches))))

    used_udfs: Set[str] = set(chosen.values())

    class ReplacementTransformer(cst.CSTTransformer):
        def leave_AnnAssign(self, original_node: cst.AnnAssign, updated_node: cst.AnnAssign) -> cst.AnnAssign:
            if isinstance(original_node.target, cst.Name):
                tgt = original_node.target.value
                if tgt in chosen:
                    udf_name = chosen[tgt]
                    # create a Call node calling udf_name with the same args
                    new_call = cst.Call(func=cst.Name(udf_name), args=original_node.value.args)
                    return updated_node.with_changes(value=new_call)
            return updated_node

    new_module = module.visit(ReplacementTransformer())

    # Add import for used UDFs if not present
    if used_udfs:
        # Check existing imports
        existing_imports: Set[str] = set()
        for stmt in new_module.body:
            if isinstance(stmt, cst.SimpleStatementLine) and stmt.body and isinstance(stmt.body[0], cst.ImportFrom):
                imp = stmt.body[0]
                if isinstance(imp.module, cst.Attribute) or isinstance(imp.module, cst.Name):
                    mod_name = new_module.code_for_node(imp.module)
                    if mod_name.strip() == "udf_insertion.dummy_udfs" or mod_name.strip().endswith("dummy_udfs"):
                        for n in imp.names:
                            if isinstance(n, cst.ImportStar):
                                existing_imports.update(used_udfs)
                            else:
                                existing_imports.add(new_module.code_for_node(n.name).strip())

        to_add = list(used_udfs - existing_imports)
        if to_add:
            # Construct ImportFrom node
            names = [cst.ImportAlias(name=cst.Name(n)) for n in sorted(to_add)]
            # Build dotted module expression `udf_insertion.dummy_udfs`
            parts = "udf_insertion.dummy_udfs".split(".")
            mod_expr: cst.BaseExpression = cst.Name(parts[0])
            for p in parts[1:]:
                mod_expr = cst.Attribute(value=mod_expr, attr=cst.Name(p))
            imp = cst.SimpleStatementLine(body=[cst.ImportFrom(module=mod_expr, names=names)])
            # Prepend import(s) to module
            new_module = new_module.with_changes(body=[imp] + list(new_module.body))

    return new_module


def _demo():
    sample = '''
x: int = some_sql_expr(1, 2)
y: str = some_sql_expr(s, 'abc')
s: int = 3
'''
    mod = cst.parse_module(sample)
    new = insert_udfs_by_types(mod, n=1, seed=0)
    print(new.code)


if __name__ == "__main__":
    _demo()
