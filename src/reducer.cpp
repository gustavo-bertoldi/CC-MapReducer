#include<iostream>
#include<fstream>
#include<unordered_map>
#include<set>
#include<algorithm>

using namespace std;

void reduce(const string &file_name) {
    ifstream input("output/" + file_name + ".shuf0");
    string line;
    unordered_map<string, set<string>> map;
    while (getline(input, line)) {
        string key = line.substr(0, line.find(':'));
        string value = line.substr(line.find(':') + 2);  
        if (map.find(key) == map.end()) map[key] = set<string>();
        map[key].insert(value);
    }

    ofstream output("output/" + file_name + ".red");
    for (auto &p: map) {
        if (p.second.size() > 1) {
            output << p.first << ": { ";
            for (auto &v: p.second) {
                output << v;
                if (v != *p.second.rbegin()) output << ", ";
            }
            output << " }" << endl;
        }
    }
}

int main() {
    reduce("10001");
    return 0;
}
