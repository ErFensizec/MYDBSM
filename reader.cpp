#include <iostream>
#include <fstream>
#include <nlohmann/json.hpp>
#include <map>
#include <reader.h>
#include <vector>
//#include <reader.h>

using namespace std;
using json = nlohmann::json;
typedef map<string, string> mss;
const int FIELD_NAME_LENGTH = 50;

typedef struct {
    char sFieldName[FIELD_NAME_LENGTH];  //字段名
    char sType[8];  //字段类型
    int iSize;  //字长
    char bKey;  //该字段是否为KEY键
    char bNullFlag;  //该字段是否允许为空
    char bValidFlag;  //该字段是否有效，可用于以后对表中该字段的删除
} TableMode, * PTableMode;


void saveDBInJson(vector<mss>db, string file_path)
{

    // 使用nlohmann::json来保存map到JSON
    nlohmann::json json_map,json_list;
    for (int i = 0; i < db.size();i++) {
        for (auto it = db[i].begin(); it != db[i].end(); ++it) {
            // 使用it->first和it->second访问键和值
            json_map[it->first] = it->second;
        }
        
    }

    // 将JSON对象输出到文件
    std::ofstream o(file_path);
    o << json_map.dump(4); // 使用4个空格缩进
    o.close();

}

mss readMapFromJsonFile(string file_path)
{
    // 打开JSON文件
    std::ifstream file(file_path);
    if (!file.is_open()) {
        std::cerr << "无法打开文件" << std::endl;
        return {};
    }

    // 从文件流解析JSON
    json j = json::parse(file);
    file.close();
    mss myMap;

    // 遍历JSON对象，并将其存入map
    for (auto& element : j.items()) {
        myMap[element.key()] = element.value();
    }

    // 输出map内容
    for (auto& pair : myMap) {
        std::cout << pair.first << " : " << pair.second << std::endl;
    }
    return myMap;

}

