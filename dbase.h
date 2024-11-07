#ifndef DBASE_H
#define DBASE_H

#include <string>
#include "Array.h"
#include "json.hpp"
#include "pair.h"
using namespace std;
using json = nlohmann::json;

// Узел списка для хранения таблиц
struct Node {
    string name;
    Array data;
    Node* next;

    Node(const string& name) : name(name), next(nullptr) {}
};

// Структура базы данных
struct dbase {
    string filename; 
    string schema_name;
    Node* head;
    int current_pk;

    dbase() : head(nullptr), current_pk(0) {}
    ~dbase();
};

// Функции для работы с dbase
void lockPrimaryKey(dbase& db);
void unlockPrimaryKey(dbase& db);
void initializePrimaryKey(dbase& db);
void createDirectories(dbase& db, const json& structure);
void addNode(dbase& db, const string& table_name);
void loadSchema(dbase& db, const string& schema_file);
void updatePrimaryKey(dbase& db);
Node* findNode(dbase& db, const string& table_name);
size_t getColumnCount(dbase& db, const string& table);
void load(dbase& db);
bool applyAndFilters(const json& entry, const MyPair<string, string> filters[], int filter_count);
bool applyOrFilters(const json& entry, const MyPair<string, string> filters[], int filter_count);
void select(dbase& db, const string& column, const string& table, const MyPair<string, string> filters[], int filter_count, const string& filter_type);
void saveSingleEntryToCSV(dbase& db, const string& table, const json& entry);
void insert(dbase& db, const string& table, json entry);
void rewriteCSV(dbase& db, const string& table);
void deleteRow(dbase& db, const string& column, const string& value, const string& table);
void executeQuery(dbase& db, const string& query);

#endif // DBASE_H
