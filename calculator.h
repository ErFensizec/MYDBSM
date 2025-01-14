#include <iostream>
#include <vector>
#include <string>
#include <sstream>
#include <regex>
#include <fstream>
#include <nlohmann/json.hpp>
#include <map>
#include <algorithm>
#include <set>
#include <string>
#include <iostream>
#include <sstream>
#include <string>
#include <unordered_map>
#include <stack>
#include <vector>
#include <cctype>

#ifndef CALCULATOR_H
#define CALCULATOR_H

// Token types
enum TokenType {
    VARIABLE, NUMBER, OPERATOR, AND, OR, NOT, LEFT_PAREN, RIGHT_PAREN
};

// Token structure
struct Token {
    TokenType type;
    std::string value;
};
//std::string trim2(const std::string& str);
bool tokenize(const std::string& expr, std::vector<Token>& tokens);
int getPrecedence(const Token& token);
bool isRightAssociative(const Token& token);
bool infixToPostfix(const std::vector<Token>& tokens, std::vector<Token>& postfix);
bool evaluatePostfix(const std::vector<Token>& postfix, const std::unordered_map<std::string, double>& variableNames, double& result);
bool parseWhereStatement(const std::string& input, const std::unordered_map<std::string, double>& variableNames, bool& result);

#endif // CALCULATOR_H
