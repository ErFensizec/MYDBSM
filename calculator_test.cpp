#include <iostream>
#include <sstream>
#include <string>
#include <unordered_map>
#include <stack>
#include <vector>
#include <cctype>

// Helper function to trim whitespace
std::string trim(const std::string& str) {
    size_t start = str.find_first_not_of(" \t");
    size_t end = str.find_last_not_of(" \t");
    return (start == std::string::npos) ? "" : str.substr(start, end - start + 1);
}

// Token types
enum TokenType {
    VARIABLE, NUMBER, OPERATOR, AND, OR, NOT, LEFT_PAREN, RIGHT_PAREN
};

// Token structure
struct Token {
    TokenType type;
    std::string value;
};

// Tokenize the expression
bool tokenize(const std::string& expr, std::vector<Token>& tokens) {
    std::istringstream stream(expr);
    std::string token;

    while (stream >> token) {
        if (token == "+" || token == "-" || token == "*" || token == "/" ||
            token == ">" || token == "<" || token == "==" || token == "(" || token == ")") {
            tokens.push_back({OPERATOR, token});
        } else if (token == "and") {
            tokens.push_back({AND, token});
        } else if (token == "or") {
            tokens.push_back({OR, token});
        } else if (token == "not") {
            tokens.push_back({NOT, token});
        } else if (std::isdigit(token[0]) || (token[0] == '-' && token.size() > 1)) {
            tokens.push_back({NUMBER, token});
        } else {
            tokens.push_back({VARIABLE, token});
        }
    }

    return true;
}

// Operator precedence and associativity
int getPrecedence(const Token& token) {
    if (token.type == NOT) return 3;  // NOT has the highest precedence
    if (token.type == AND) return 2;  // AND has higher precedence than OR
    if (token.type == OR) return 1;   // OR has the lowest precedence
    if (token.type == OPERATOR) return 2; // Arithmetic operators share medium precedence
    return 0;
}

bool isRightAssociative(const Token& token) {
    return token.type == NOT; // NOT is right-associative
}

// Convert infix to postfix using Shunting-yard algorithm
bool infixToPostfix(const std::vector<Token>& tokens, std::vector<Token>& postfix) {
    std::stack<Token> opStack;
    for (const auto& token : tokens) {
        if (token.type == NUMBER || token.type == VARIABLE) {
            postfix.push_back(token);
        } else if (token.type == OPERATOR || token.type == AND || token.type == OR || token.type == NOT) {
            while (!opStack.empty() && opStack.top().type != LEFT_PAREN &&
                   (getPrecedence(opStack.top()) > getPrecedence(token) ||
                    (getPrecedence(opStack.top()) == getPrecedence(token) && !isRightAssociative(token)))) {
                postfix.push_back(opStack.top());
                opStack.pop();
            }
            opStack.push(token);
        } else if (token.type == LEFT_PAREN) {
            opStack.push(token);
        } else if (token.type == RIGHT_PAREN) {
            while (!opStack.empty() && opStack.top().type != LEFT_PAREN) {
                postfix.push_back(opStack.top());
                opStack.pop();
            }
            if (opStack.empty()) return false; // Unbalanced parentheses
            opStack.pop(); // Remove LEFT_PAREN
        }
    }

    while (!opStack.empty()) {
        postfix.push_back(opStack.top());
        opStack.pop();
    }

    return true;
}

// Evaluate the postfix expression
bool evaluatePostfix(const std::vector<Token>& postfix, const std::unordered_map<std::string, double>& variableNames, double& result) {
    std::stack<double> evalStack;
    for (const auto& token : postfix) {
        if (token.type == NUMBER) {
            evalStack.push(std::stod(token.value));
        } else if (token.type == VARIABLE) {
            if (variableNames.find(token.value) == variableNames.end()) {
                std::cerr << "Error: Variable not found: " << token.value << std::endl;
                return false;
            }
            evalStack.push(variableNames.at(token.value));
        } else if (token.type == OPERATOR || token.type == AND || token.type == OR || token.type == NOT) {
            if (token.type == NOT) {
                if (evalStack.empty()) {
                    std::cerr << "Error: Insufficient values for NOT operation" << std::endl;
                    return false;
                }
                double a = evalStack.top(); evalStack.pop();
                evalStack.push(a == 0 ? 1.0 : 0.0); // NOT logic
            } else {
                if (evalStack.size() < 2) {
                    std::cerr << "Error: Insufficient values in expression" << std::endl;
                    return false;
                }
                double b = evalStack.top(); evalStack.pop();
                double a = evalStack.top(); evalStack.pop();
                if (token.value == "+") evalStack.push(a + b);
                else if (token.value == "-") evalStack.push(a - b);
                else if (token.value == "*") evalStack.push(a * b);
                else if (token.value == "/") evalStack.push(a / b);
                else if (token.value == ">") evalStack.push(a > b ? 1.0 : 0.0);
                else if (token.value == "<") evalStack.push(a < b ? 1.0 : 0.0);
                else if (token.value == "==") evalStack.push(a == b ? 1.0 : 0.0);
                else if (token.value == "and") evalStack.push((a != 0 && b != 0) ? 1.0 : 0.0);
                else if (token.value == "or") evalStack.push((a != 0 || b != 0) ? 1.0 : 0.0);
                else {
                    std::cerr << "Error: Unknown operator: " << token.value << std::endl;
                    return false;
                }
            }
        }
    }

    if (evalStack.size() != 1) {
        std::cerr << "Error: Invalid expression" << std::endl;
        return false;
    }

    result = evalStack.top();
    return true;
}

// Main function to parse and evaluate WHERE statement
bool parseWhereStatement(const std::string& input, const std::unordered_map<std::string, double>& variableNames, bool& result) {
    if (input.size() < 6 || input.substr(0, 6) != "WHERE ") {
        return false;
    }

    std::string expr = trim(input.substr(6));
    if (expr.back() != ';') {
        return false;
    }

    expr.pop_back(); // Remove trailing ';'
    std::vector<Token> tokens, postfix;
    if (!tokenize(expr, tokens)) {
        return false;
    }

    if (!infixToPostfix(tokens, postfix)) {
        return false;
    }

    double numericResult;
    if (!evaluatePostfix(postfix, variableNames, numericResult)) {
        return false;
    }

    result = (numericResult != 0);
    return true;
}

int main() {
    std::unordered_map<std::string, double> variableNames = {
        {"Age", 9},
        {"y", 10},
        {"z", 15}
    };

    std::string input = "WHERE Age==9;";
    bool result;

    if (parseWhereStatement(input, variableNames, result)) {
        std::cout << "Expression result: " << (result ? "true" : "false") << std::endl;
    } else {
        std::cout << "Failed to parse WHERE statement." << std::endl;
    }

    return 0;
}

