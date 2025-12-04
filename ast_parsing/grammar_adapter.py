from collections import Counter
from ast_parsing import SparkSnippetSummary
from ast_parsing import SparkCallVisitor

class GrammarAdapter:
    def features_from_summary(self, s: SparkSnippetSummary) -> Counter:
        feats = Counter()

        #1. DataFrame ops: DF_OP::select, DF_OP::groupBy, etc.
        for op in s.pyspark_ops:
            feats[f"DF_OP::{op}"] += 1

        #2. Third-party libs
        for lib in s.third_party_libs:
            feats[f"DF_THIRD_PARTY_LIB::{lib}"] += 1

        #3. UDFs (I'm going to treat as UDF MAP nodes, plus any metadata from UDFInfo)
        for name, udf in s.udfs.items():
            feats["UDF_DEF"] += 1
            feats["DF_UDF_MAP"] += 1
            feats[f"UDF_NAME::{name}"] += 1

            #aside: if UDFInfo has third_party_dependencies, other random stuffs, etc.
            deps = getattr(udf, "third_party_dependencies", set()) or set()
            for d in deps:
                feats[f"UDF_THIRD_PARTY_LIB::{d}"] += 1

            #TODO: bucket by annotation or return type here if UDFInfo has it -- what metrics? skip for now.

        return feats

    #Accept visitor directly.
    def features_from_visitor(self, visitor: "SparkCallVisitor") -> Counter:
        return self.features_from_summary(visitor.to_summary())