#include <iostream>
#include <fstream>
#include <nlohmann/json.hpp>
#include <map>
using namespace std;
using json = nlohmann::json;
typedef map<string, string> mss;
#ifndef READER_H
#define READER_H

void saveMapInJson(mss my_map, string file_path);
mss readMapFromJsonFile(string file_path);

#endif // READER_H