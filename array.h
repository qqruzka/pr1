#pragma once
template<typename T>
class Vector {
private:
    T* arr;
    size_t capacity;
    size_t size;

public:
    Vector() : capacity(10), size(0) {
        arr = new T[capacity];
    }

    ~Vector() {
        delete[] arr;
    }

    void addEnd(const T& value) {
        if (size >= capacity) {
            capacity *= 2;
            T* new_arr = new T[capacity];
            for (size_t i = 0; i < size; ++i) {
                new_arr[i] = arr[i];
            }
            delete[] arr;
            arr = new_arr;
        }
        arr[size++] = value;
    }

    T get(size_t index) const {
        if (index >= size) throw std::out_of_range("Index out of range");
        return arr[index];
    }

    size_t getSize() const {
        return size;
    }
};
