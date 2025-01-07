#include <iostream>
#include <fstream>
#include <nlohmann/json.hpp>
#include <map>

using namespace std;
using json = nlohmann::json;
typedef map<string, string> mss;

void saveMapInJson(mss my_map, string file_path)
{

    // 使用nlohmann::json来保存map到JSON
    nlohmann::json json_map;
    for (const auto& pair : my_map) {
        json_map[pair.first] = pair.second;
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

void creat

int main() {
    // 创建一个map
    mss my_map = { {"apple", "5"}, {"banana", "8"}, {"cherry", "2"} };
    saveMapInJson(my_map, "output.json");
    mss data1=readMapFromJsonFile("output.json");
    return 0;
}