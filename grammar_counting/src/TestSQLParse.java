import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.tree.*;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import org.example.SqlBaseParser;
import org.example.SqlBaseLexer;

public class TestSQLParse {
    public static void main(String[] args) throws Exception {
        String inputFile = "/home/pmukho/classes/cs230/proj/grammar_counting/test.txt"; // your SQL file
        InputStream is = new FileInputStream(inputFile);

        // Create CharStream from file
        CharStream input = CharStreams.fromStream(is);

        // Lexer
        SqlBaseLexer lexer = new SqlBaseLexer(input);
        CommonTokenStream tokens = new CommonTokenStream(lexer);

        // Parser
        SqlBaseParser parser = new SqlBaseParser(tokens);

        // Parse with top-level rule (replace with your grammar's start rule)
        ParseTree tree = parser.compoundOrSingleStatement();

        // Walk the tree with RuleCountingListener
        ParseTreeWalker walker = new ParseTreeWalker();
        RuleCountingListener listener = new RuleCountingListener();
        walker.walk(listener, tree);

        // Print rule counts
        System.out.println("Rule Counts:");
        for (Map.Entry<String, Integer> entry : listener.getRuleCounts().entrySet()) {
            System.out.println(entry.getKey() + ": " + entry.getValue());
        }

        // Print parse tree
        // System.out.println(tree.toStringTree(parser));
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
