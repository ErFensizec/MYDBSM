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
#include <iostream>
#include <string>
#include <unordered_map>
#include <stack>
#include <sstream>
#include <cctype>
#include <stdexcept>

#ifndef CALCULATOR2_H
#define CALCULATOR2_H

double getVariableValue(const std::unordered_map<std::string, double>& variableNames, const std::string& var);
int precedence(const std::string& op);
bool applyOperator(const std::string& op, std::stack<double>& values);
std::vector<std::string> infixToPostfix(const std::string& expression);
bool evaluatePostfix(const std::vector<std::string>& postfix, const std::unordered_map<std::string, double>& variableNames);
bool parseWhereStatement(const std::string& input, const std::unordered_map<std::string, double>& variableNames);
#endif // CALCULATOR2_H
