#include<iostream>
#include<fstream>
#include<vector>
#include<unordered_set>
#include<algorithm>
#include<sstream> 

using namespace std;

const unordered_set<string>* read_stop_words() {
    unordered_set<string>* stop_words = new unordered_set<string>();
    ifstream input("stop_words.txt");
    string stop_word;
    while(getline(input, stop_word, ',')) {
        stop_words->insert(stop_word);
    }
    return stop_words;
}

bool is_letter(const char &c) {
    return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z');
}

void read_from_file(const string &file_name, const unordered_set<string> &stop_words = unordered_set<string>()) {
    ifstream input(file_name);
    ofstream output(file_name.substr(0, file_name.find_last_of('.')) + ".map");
    string line;
    while (getline(input, line)) {
        stringstream words(line);
        string word;
        while (words >> word) {
            string filtered, ordered;
            copy_if(word.begin(), word.end(), back_inserter(filtered), ::is_letter);
            transform(filtered.begin(), filtered.end(), filtered.begin(), ::tolower);
            if (filtered.length() != 0 && stop_words.find(filtered) == stop_words.end()) {
                ordered = filtered;
                sort(ordered.begin(), ordered.end());
                output << ordered << ": " << filtered << endl;
            }
        }      
    }
}

int main() {
    const unordered_set<string>* stop_words = read_stop_words();
    read_from_file("source_files/10001.txt", *stop_words);
    return 0;
} 
