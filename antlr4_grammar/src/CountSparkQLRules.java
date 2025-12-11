import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.atn.*;
import org.antlr.v4.runtime.dfa.DFA;
import org.antlr.v4.runtime.tree.*;
import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Stream;
import java.util.Map;
import org.example.SqlBaseParser;
import org.example.SqlBaseLexer;

public class CountSparkQLRules {
    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("Usage: java TestSQLParse <directory holding input files or single file>");
            return;
        }

        Path inputPath = Path.of(args[0]);
        if (!Files.exists(inputPath)) {
            System.err.println("Not a valid directory or file: " + inputPath);
            return;
        }

        // Aggregate counts
        Map<String, Integer> aggregateCounts = new HashMap<>();
        int filesProcessed = 0;
        int filesWithErrors = 0;

        // Collect input files
        List<Path> inputFiles = new ArrayList<>();
        if (Files.isDirectory(inputPath)) {
            try (Stream<Path> paths = Files.walk(inputPath)) {
                paths
                    .filter(Files::isRegularFile)
                    .filter(p -> p.toString().endsWith(".sql"))
                    .forEach(inputFiles::add);
            }
        } else if (inputPath.toString().endsWith(".sql")) {
            inputFiles.add(inputPath);
        } else {
            System.err.println("Input file is not a .sql file: " + inputPath);
            return;
        }

        // Process each file
        for (Path filePath : inputFiles) {
            try (InputStream is = new FileInputStream(filePath.toFile())) {
                // Create CharStream from file
                CharStream input = CharStreams.fromStream(is);

                // Lexer
                SqlBaseLexer lexer = new SqlBaseLexer(input);
                CommonTokenStream tokens = new CommonTokenStream(lexer);

                // Parser
                SqlBaseParser parser = new SqlBaseParser(tokens);

                // Add error listener
                AnyErrorListener errorListener = new AnyErrorListener();
                parser.removeErrorListeners(); // Remove default ConsoleErrorListener
                parser.addErrorListener(errorListener);

                // Parse with top-level rule (replace with your grammar's start rule)
                ParseTree tree = parser.compoundOrSingleStatement();

                // Check for syntax errors
                if (errorListener.hasErrors()) {
                    System.err.println("Syntax errors in file: " + filePath);
                    filesWithErrors++;
                    continue;
                }

                // Walk the tree with RuleCountingListener
                ParseTreeWalker walker = new ParseTreeWalker();
                RuleCountingListener listener = new RuleCountingListener();
                walker.walk(listener, tree);                

                // Aggregate rule counts
                for (Map.Entry<String, Integer> entry : listener.getRuleCounts().entrySet()) {
                    aggregateCounts.put(entry.getKey(),
                        aggregateCounts.getOrDefault(entry.getKey(), 0) + entry.getValue());
                }
                filesProcessed++;
            } catch (Exception e) {
                System.err.println("Error processing file " + filePath);
                filesWithErrors++;
            }
        }

        // Print summary
        System.out.println("Files processed: " + filesProcessed);
        System.out.println("Files with errors: " + filesWithErrors);
        System.out.println("Rule counts:");
        for (Map.Entry<String, Integer> entry : aggregateCounts.entrySet()) {
            System.out.println(entry.getKey() + ": " + entry.getValue());
        }
    }
}

class AnyErrorListener implements ANTLRErrorListener {
    private boolean hasErrors = false;

    @Override
    public void syntaxError(Recognizer<?, ?> recognizer, Object offendingSymbol,
                            int line, int charPositionInLine,
                            String msg, RecognitionException e) {
        hasErrors = true;
    }

    @Override
    public void reportAmbiguity(Parser recognizer, DFA dfa, int startIndex,
                                int stopIndex, boolean exact, BitSet ambigAlts,
                                ATNConfigSet configs) {
        // No action needed for ambiguities
    }

    @Override
    public void reportAttemptingFullContext(Parser recognizer, DFA dfa,
                                            int startIndex, int stopIndex,
                                            BitSet conflictingAlts,
                                            ATNConfigSet configs) {
        // No action needed for full context attempts
    }

    @Override
    public void reportContextSensitivity(Parser recognizer, DFA dfa,
                                         int startIndex, int stopIndex,
                                         int prediction,
                                         ATNConfigSet configs) {
        // No action needed for context sensitivities
    }

    public boolean hasErrors() {
        return hasErrors;
    }
}

class RuleCountingListener implements ParseTreeListener {

    private Map<String, Integer> ruleCounts = new HashMap<>();

    // Increment count for a rule
    private void countRule(String ruleName) {
        ruleCounts.put(ruleName, ruleCounts.getOrDefault(ruleName, 0) + 1);
    }

    @Override
    public void enterEveryRule(ParserRuleContext ctx) {
        // Get ANTLR-generated class name for the rule
        String ruleName = ctx.getClass().getSimpleName(); // e.g., SqlBaseParser.CompoundOrSingleStatementContext
        countRule(ruleName);
    }

    @Override
    public void exitEveryRule(ParserRuleContext ctx) {
        // No action needed on exit
    }

    @Override
    public void visitTerminal(TerminalNode node) {
        // No action needed for terminals
    }

    @Override
    public void visitErrorNode(ErrorNode node) {
        // No action needed for error nodes
    }

    public Map<String, Integer> getRuleCounts() {
        return ruleCounts;
    }
}
