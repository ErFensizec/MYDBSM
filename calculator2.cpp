#include <iostream>
#include <string>
#include <unordered_map>
#include <stack>
#include <sstream>
#include <cctype>
#include <stdexcept>
#include <vector>

// 获取变量值的函数
double getVariableValue(const std::unordered_map<std::string, double>& variableNames, const std::string& var) {
    if (variableNames.find(var) == variableNames.end()) {
        throw std::runtime_error("Variable not found: " + var);
    }
    return variableNames.at(var);
}

// 运算优先级
int precedence(const std::string& op) {
    if (op == "NOT") return 4;
    if (op == "*" || op == "/" || op == "%") return 3;
    if (op == "+" || op == "-") return 2;
    if (op == ">" || op == "<" || op == "=") return 1;
    if (op == "AND" || op == "OR") return 0;
    return -1;
}

// 执行操作
bool applyOperator(const std::string& op, std::stack<double>& values) {
    if (op == "NOT") {
        if (values.empty()) return false;
        double a = values.top(); values.pop();
        values.push((a == 0) ? 1 : 0);
    }
    else {
        if (values.size() < 2) return false;
        double b = values.top(); values.pop();
        double a = values.top(); values.pop();
        if (op == "+") values.push(a + b);
        else if (op == "-") values.push(a - b);
        else if (op == "*") values.push(a * b);
        else if (op == "/") values.push(a / b);
        else if (op == ">") values.push((a > b) ? 1 : 0);
        else if (op == "<") values.push((a < b) ? 1 : 0);
        else if (op == "=") values.push((a == b) ? 1 : 0);
        else if (op == "%" && int(a) == a && int(b) == b) values.push((int(a) % int(b)));
        else if (op == "AND") values.push(a && b);
        else if (op == "OR") values.push(a || b);
        else {
            values.push(a);
            values.push(b);
            return false;
        }
    }
    return true;
}

// 中缀表达式转后缀表达式
std::vector<std::string> infixToPostfix(const std::string& expression) {
    std::istringstream iss(expression);
    std::vector<std::string> postfix;
    std::stack<std::string> ops;

    std::string token;
    while (iss >> token) {
        if (std::isdigit(token[0]) || std::isalpha(token[0])) {
            postfix.push_back(token);
        }
        else if (token == "(") {
            ops.push(token);
        }
        else if (token == ")") {
            while (!ops.empty() && ops.top() != "(") {
                postfix.push_back(ops.top());
                ops.pop();
            }
            if (!ops.empty()) ops.pop(); // 弹出左括号
        }
        else { // 操作符
            while (!ops.empty() && precedence(ops.top()) >= precedence(token)) {
                postfix.push_back(ops.top());
                ops.pop();
            }
            ops.push(token);
        }
    }

    while (!ops.empty()) {
        postfix.push_back(ops.top());
        ops.pop();
    }

    return postfix;
}

// 计算后缀表达式
bool evaluatePostfix(const std::vector<std::string>& postfix, const std::unordered_map<std::string, double>& variableNames) {
    std::stack<double> values;

    for (const auto& token : postfix) {
        if (std::isdigit(token[0])) {
            values.push(std::stod(token));
        }
        else if (std::isalpha(token[0])) {
            if (!applyOperator(token, values)) {
                values.push(getVariableValue(variableNames, token));
            }
        }
        else {
            if (!applyOperator(token, values)) {
                throw std::runtime_error("Invalid operator or insufficient values for: " + token);
            }
        }
    }

    if (values.size() != 1) {
        throw std::runtime_error("Invalid expression");
    }

    return values.top() != 0;
}

// 主分析函数
bool parseWhereStatement(const std::string& input, const std::unordered_map<std::string, double>& variableNames) {
    if (input.size() < 6 || input.substr(0, 6) != "WHERE " || input.back() != ';') {
        throw std::runtime_error("Invalid WHERE clause syntax");
    }

    std::string expression = input.substr(6, input.size() - 7); // 去掉 "WHERE " 和 ";"
    auto postfix = infixToPostfix(expression);
    return evaluatePostfix(postfix, variableNames);
}
/*
// 测试
int main() {
    //std::cout<<"value is:"<<3%5*2;
    std::unordered_map<std::string, double> variableNames = {
        {"x", 10},
        {"y", 5},
        {"z", 20}
    };

    std::string input = "WHERE ( x + y ) * z > 100 AND NOT ( y < 2 );";
    std::string input2 = "WHERE x = 9;";
    try {
        bool result = parseWhereStatement(input, variableNames);
        std::cout << "Expression result: " << std::boolalpha << result << std::endl;
    }
    catch (const std::exception& ex) {
        std::cout << "Error: " << ex.what() << std::endl;
    }

    return 0;
}
*/