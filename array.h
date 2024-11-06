#ifndef ARRAY_H
#define ARRAY_H

#include <string>
#include <stdexcept>

using namespace std;

struct Array {
    string* arr;
    size_t capacity;
    size_t size;

    Array();
    ~Array();
    void addEnd(const string& value);
    string get(size_t index) const;
    size_t getSize() const;
};

#endif // ARRAY_H
