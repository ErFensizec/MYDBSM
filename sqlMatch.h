#ifndef SQLMATCH_H
#define SQLMATCH_H

#include <string>
#include <algorithm>
#include <cctype>

// ת���ַ���ΪСд
inline std::string toLowerCase(const std::string& str) {
    std::string lowerStr = str;
    std::transform(lowerStr.begin(), lowerStr.end(), lowerStr.begin(), ::tolower);
    return lowerStr;
}

// ת���ַ���Ϊ��д
inline std::string toUpperCase(const std::string& str) {
    std::string upperStr = str;
    std::transform(upperStr.begin(), upperStr.end(), upperStr.begin(), ::toupper);
    return upperStr;
}

// �ж��Ƿ�������ַ��������Դ�Сд��
inline bool containsIgnoreCase(const std::string& text, const std::string& keyword) {
    std::string lowerText = toLowerCase(text);
    std::string lowerKeyword = toLowerCase(keyword);
    return lowerText.find(lowerKeyword) != std::string::npos;
}

#endif // STRING_UTILS_H
#pragma once
