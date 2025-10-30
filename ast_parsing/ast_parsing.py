import json
import libcst as cst
from pathlib import Path
import collections
from typing import List, Dict, Any, Union, Optional, Generator, Tuple
from object_types import UDFInfo, AnalysisResult

def extract_snippets(filename: Union[str, Path]) -> Generator[Tuple[str, str, Dict[str, Any]], None, None]:
    """
    reading the new-format input and yield (path, snippet, metadata) tuples.

    example of meta is {"calls": [{"library": "random", "call": "randint"}, ...]} [from scraping readme doc]
    """
    with open(filename, encoding="utf-8") as f:
        #print("Opened file:", filename)
        for line in f:
            if not line.strip():
                continue
            try:
                entry = json.loads(line)
                repo_name = entry.get("repo_name", "<unknown>")
                clone_url = entry.get("clone_url")
                for file_obj in entry.get("files", []) or []:
                    file_path = file_obj.get("path", "<unknown_path>")

                    #process new udf entries
                    for udf in file_obj.get("udfs", []):
                        snippet = udf.get("definition", "").strip()
                        if snippet:
                            meta = { #attach third party lib specifically to that UDFInfo. 
                                "udf_name": udf.get("name"),
                                "calls": udf.get("calls", []) or [],
                                "path": file_path,
                                "clone_url": clone_url,
                                "repo_name": repo_name,
                            }
                            yield repo_name, snippet, meta

                    #otherwise is df expr
                    for expr in file_obj.get("df_exprs", []):
                        snippet = expr.strip() if isinstance(expr, str) else ""
                        if snippet:
                            meta = {
                                    "path": file_path,
                                    "clone_url": clone_url,
                                    "repo_name": repo_name,
                                }
                            yield repo_name, snippet, meta

            except json.JSONDecodeError as e:
                print(f"Skipping invalid JSON line: {e}")


def try_parse(snippet: str) -> Optional[cst.CSTNode]:
    """Try parsing a code snippet using libcst. TODO: check if we want to use a different library, but this seems pretty robust for our usecase so far."""
    try:
        return cst.parse_module(snippet)
    except Exception:
        return None

class SparkCallVisitor(cst.CSTVisitor):
    """Collects DataFrame operations and UDF usages."""
    PYSPARK_DF_METHODS = {
        'alias', 'agg', 'collect', 'count', 'distinct', 'drop', 'dropDuplicates', 'dropna',
        'filter', 'fillna', 'groupBy', 'join', 'intersect', 'limit', 'orderBy', 'pivot',
        'rollup', 'cube', 'select', 'selectExpr', 'sort', 'sample', 'sampleBy', 'subtract',
        'union', 'unionAll', 'where', 'withColumn', 'withColumnRenamed', 'exceptAll'
    }
    THIRD_PARTY_LIBS = {"numpy", "pandas", "torch", "sklearn"}

    def __init__(self):
        self.funcs = set()
        self.third_party_libs = set()
        self.third_party_lib_freq = collections.defaultdict(int) #TODO: other metrics.
        self.udfs: Dict[str, UDFInfo] = {}
        self.known_udfs = set()
    
    @property
    def has_udf(self) -> bool:
        return bool(self.udfs)

    def visit_Assign(self, node: cst.Assign):
        """Detects UDF assignments including tuple unpacking."""

        #we need to recursively extract variable names from targets
        def _get_target_names(target):
            if isinstance(target, cst.Name):
                return [target.value]
            elif isinstance(target, cst.Tuple):
                names = []
                for elem in target.elements:
                    names.extend(_get_target_names(elem.value))
                return names
            return []

        #extend to process multiple targets
        for t in node.targets:
            target_names = _get_target_names(t.target)
            if not target_names:
                continue

            #Case 1: normal case of assigning UDF
            if isinstance(node.value, cst.Call):
                code_repr = cst.Module([]).code_for_node(node.value.func)
                if "udf" in code_repr:
                    for name in target_names:
                        self.udfs[name] = UDFInfo(name=name)
                        self.known_udfs.add(name)
                    continue

            #Case 2: Edge case of aliasing existing UDF
            if isinstance(node.value, cst.Name) and node.value.value in self.udfs:
                for name in target_names:
                    self.udfs[name] = self.udfs[node.value.value]
                    self.known_udfs.add(name)
            
            #TODO: Identify other cases that might be there, I'm prob missing smth.
    
    def _is_udf_call(self, call_node: cst.CSTNode) -> bool:
        """
        Detect if a node represents a udf factory/callee:
        - Name('udf')
        - Attribute(..., 'udf') like  pyspark.sql.functions.udf or F.udf
        - Call whose callee is one of the above like a normal decorator @udf(...)
        This way it is more robust to decorators being diff types of nodes.
        """
        # If it's a Call, inspect the callee
        if isinstance(call_node, cst.Call):
            callee = call_node.func
        else:
            callee = call_node

        #direct name of udf (what we had before)
        if isinstance(callee, cst.Name) and callee.value == "udf":
            return True

        # Attribute ending with .udf
        if isinstance(callee, cst.Attribute) and getattr(callee.attr, "value", "") == "udf":
            return True

        #To be thorough: stringify the callee and look for '.udf' or 'udf' token at the end
        try:
            code = cst.Module([]).code_for_node(callee)
            # check common patterns like "pyspark.sql.functions.udf" or "F.udf"
            if ".udf" in code or code.strip() == "udf":
                return True
        except Exception:
            pass

        return False

    def visit_FunctionDef(self, node: cst.FunctionDef):
        """Detect @udf decorator (all types!)"""
        for dec in node.decorators:
            dec_node = dec.decorator  # types include Name, Attribute, or Call
            # Case: @udf
            if isinstance(dec_node, cst.Name) and dec_node.value == "udf":
                self.udfs[node.name.value] = UDFInfo(name=node.name.value, decorator=True)
                self.known_udfs.add(node.name.value)
                return

            # Case: @udf(...) or other Call wrapping a udf callee
            if isinstance(dec_node, cst.Call) and self._is_udf_call(dec_node.func):
                self.udfs[node.name.value] = UDFInfo(name=node.name.value, decorator=True)
                self.known_udfs.add(node.name.value)
                return

            # Case: @something.udf
            if isinstance(dec_node, cst.Attribute) and getattr(dec_node.attr, "value", "") == "udf":
                self.udfs[node.name.value] = UDFInfo(name=node.name.value, decorator=True)
                self.known_udfs.add(node.name.value)
                return
    
    #find udfs and chained DF ops.
    def visit_Call(self, node: cst.Call):
        """Detect PySpark df operations and UDF usages"""
        
        #detecting method calls on DF
        if isinstance(node.func, cst.Attribute):
            func_name = node.func.attr.value
            if func_name in self.PYSPARK_DF_METHODS:
                self.funcs.add(func_name)
                #checking metrics (marking known udfs)
                for arg in node.args:
                    self._mark_udf_applied(arg.value)

        #direct udf calls (pass on to visit_Assign)
        code_repr = cst.Module([]).code_for_node(node.func)
        if "udf" in code_repr:
            pass

        #Special case: spark.udf.register(  )
        if "udf.register" in code_repr:
            if len(node.args) >= 2:
                udf_name_node = node.args[1].value
                udf_name = getattr(udf_name_node, "value", None)
                if udf_name and udf_name in self.udfs:
                    self.udfs[udf_name].registered_sql = True
    
    def _mark_udf_applied(self, node):
        """Recursively mark UDFs used in calls"""
        if isinstance(node, cst.Call):
            func_name = getattr(node.func, "value", None)
            if func_name in self.udfs:
                self.udfs[func_name].applied_to_df = True
            for arg in node.args:
                self._mark_udf_applied(arg.value)

    #TODO: define what third party libraries we actually care abt.
    def visit_Import(self, node):
        for alias in node.names:
            name = alias.name.value
            if name in self.THIRD_PARTY_LIBS: 
                self.third_party_libs.add(name)
                self.third_party_lib_freq[name] += 1 #track more metrics

    def visit_ImportFrom(self, node):
        module_name = getattr(node.module, "value", "")
        if module_name in self.THIRD_PARTY_LIBS:
            self.third_party_libs.add(module_name)
            self.third_party_lib_freq[module_name] += 1 #track more metrics

def analyze_file(filename) -> List[AnalysisResult]:
    """Parse JSONL and analyze each snippet, return as new dataclass AnalysisResult."""
    output = []
    for repo_name, snippet, meta in extract_snippets(filename):
        tree = try_parse(snippet)
        if not tree:
            continue

        visitor = SparkCallVisitor()
        tree.visit(visitor)

        #add new metadata.
        calls = meta.get("calls", []) if isinstance(meta, dict) else []
        for call in calls:
            lib = call.get("library")
            if lib:
                visitor.third_party_libs.add(lib)
                visitor.third_party_lib_freq[lib] += 1
        
        #attach declared libs to that UDFInfo
        udf_name = meta.get("udf_name") if isinstance(meta, dict) else None
        if udf_name and calls:
            declared_libs = {c.get("library") for c in calls if c.get("library")}
            if udf_name in visitor.udfs:
                visitor.udfs[udf_name].third_party_dependencies.update(declared_libs)
            else:
                #add it now if didn't exist ebfore.
                visitor.udfs[udf_name] = UDFInfo(name=udf_name, decorator=False, snippet=snippet)
                visitor.udfs[udf_name].third_party_dependencies.update(declared_libs)
                visitor.known_udfs.add(udf_name)

        path = meta.get("path", "<unknown_path>") if isinstance(meta, dict) else "<unknown_path>"
        clone_url = meta.get("clone_url") if isinstance(meta, dict) else None

        if visitor.funcs or visitor.has_udf or visitor.third_party_libs:
            res = AnalysisResult(
                repo_name=repo_name,
                clone_url=clone_url,
                path=path,
                snippet=snippet,
                pyspark_ops=sorted(visitor.funcs),
                udfs={name: (udf.to_dict() if hasattr(udf, "to_dict") else repr(udf))
                      for name, udf in visitor.udfs.items()},
                third_party_libs=sorted(visitor.third_party_libs),
            )
            output.append(res)
    return output


# if __name__ == "__main__":
#     #filename = Path(__file__).parent.parent / "scraping" / "results" / "all_results.jsonl"
#     filename = Path(__file__).parent.parent / "ast_parsing" / "sample_results.jsonl"
#     analyze_file(filename)