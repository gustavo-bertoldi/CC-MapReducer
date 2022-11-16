#include<iostream>
#include<fstream>
#include<vector>
#include<algorithm>

using namespace std;

void read_from_file(const string &file_name, vector<ofstream> &outputs) {
    ifstream input(file_name + ".map");
    string line;
    while (getline(input, line)) {
        int output_idx = ::hash<string>{}(line) % 5;
        outputs[output_idx] << line << endl;   
    }
}

int main() {
    const string input = "source_files/10001";
    vector<ofstream> outputs;
    for (int i = 0; i < 5; i++) {
        outputs.push_back(ofstream(input + ".shuf" + to_string(i)));
    }

    read_from_file("source_files/10001", outputs);
    return 0;
}
