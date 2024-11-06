#ifndef DBASE_H
#define DBASE_H

#include <iostream>
#include <fstream>
#include <direct.h>
#include "json.hpp"
#include <iomanip>
#include <stdexcept>
#include "array.h"

using namespace std;
using json = nlohmann::json;

struct Node {
    string name;
    Array data;
    Node* next;

    Node(const string& name);
};

struct dbase {
    string filename; 
    string schema_name;
    Node* head;
    int current_pk;

    dbase();
    ~dbase();

    void lockPrimaryKey();
    void unlockPrimaryKey();
    void loadSchema(const string& schema_file);
    void initializePrimaryKey();
    void updatePrimaryKey();
    size_t getColumnCount(const string& table);
    void createDirectories(const json& structure);
    void addNode(const string& table_name);
    Node* findNode(const string& table_name);
    void load();
    bool applyAndFilters(const json& entry, const pair<string, string> filters[], int filter_count);
    bool applyOrFilters(const json& entry, const pair<string, string> filters[], int filter_count);
    void select(const string& column, const string& table, const pair<string, string> filters[], int filter_count, const string& filter_type);
    void saveSingleEntryToCSV(const string& table, const json& entry);
    void insert(const string& table, json entry);
    void deleteRow(const string& column, const string& value, const string& table);
    void rewriteCSV(const string& table);
};

void executeQuery(dbase& db, const string& query);

#endif // DBASE_H
