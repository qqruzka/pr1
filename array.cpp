#include "array.h"

Array::Array() : capacity(10), size(0) {
    arr = new string[capacity];
}

Array::~Array() {
    delete[] arr;
}

void Array::addEnd(const string& value) {
    if (size >= capacity) {
        capacity *= 2;
        string* new_arr = new string[capacity];
        for (size_t i = 0; i < size; ++i) {
            new_arr[i] = arr[i];
        }
        delete[] arr;
        arr = new_arr;
    }
    arr[size++] = value;
}

string Array::get(size_t index) const {
    if (index >= size) throw out_of_range("Index out of range");
    return arr[index];
}

size_t Array::getSize() const {
    return size;
}
