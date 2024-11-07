#include "dbase.h"
#include <iostream>
#include <fstream>
#include <sys/stat.h>
#include <stdexcept>
#include "pair.h"

using namespace std;

dbase::~dbase() {
    while (head) {
        Node* temp = head;
        head = head->next;
        delete temp;
    }
}

void lockPrimaryKey(dbase& db) {
    try {
        string pk_filename = db.schema_name + "/table_pk_sequence.txt";
        ofstream pk_file(pk_filename);
        if (pk_file) {
            pk_file << db.current_pk << "\nlocked";
        } else {
            throw runtime_error("Failed to lock primary key file: " + pk_filename);
        }
    } catch (const exception& e) {
        cout << "Error: " << e.what() << endl;
    }
}

void unlockPrimaryKey(dbase& db) {
    try {
        string pk_filename = db.schema_name + "/table_pk_sequence.txt";
        ofstream pk_file(pk_filename);
        if (pk_file) {
            pk_file << db.current_pk << "\nunlocked";
        } else {
            throw runtime_error("Failed to unlock primary key file: " + pk_filename);
        }
    } catch (const exception& e) {
        cout << "Error: " << e.what() << endl;
    }
}

void initializePrimaryKey(dbase& db) {
    try {
        string pk_filename = db.schema_name + "/table_pk_sequence.txt";
        ifstream pk_file(pk_filename);
        
        if (pk_file) {
            pk_file >> db.current_pk;
        } else {
            db.current_pk = 0;
            ofstream pk_file_out(pk_filename);
            if (pk_file_out) {
                pk_file_out << db.current_pk << "\nunlocked";
            } else {
                throw runtime_error("Failed to create file: " + pk_filename);
            }
        }
    } catch (const exception& e) {
        cout << "Error: " << e.what() << endl;
    }
}

void createDirectories(dbase& db, const json& structure) {
    try {
        mkdir(db.schema_name.c_str(), 0777); // Для Linux

        for (const auto& table : structure.items()) {
            string table_name = table.key();
            string table_path = db.schema_name + "/" + table_name;

            mkdir(table_path.c_str(), 0777); // Права доступа для Linux
            string filename = table_path + "/1.csv";

            ifstream check_file(filename);
            if (!check_file) {
                ofstream file(filename);
                if (file.is_open()) {
                    auto& columns = table.value();
                    for (size_t i = 0; i < columns.size(); ++i) {
                        file << setw(10) << left << columns[i].get<string>() << (i < columns.size() - 1 ? ", " : "");
                    }
                    file << "\n";
                    file.close();
                }
            }

            initializePrimaryKey(db);
        }
    } catch (const exception& e) {
        cout << "Error: " << e.what() << endl;
    }
}

void addNode(dbase& db, const string& table_name) {
    Node* new_node = new Node(table_name);
    new_node->next = db.head;
    db.head = new_node;
}

void loadSchema(dbase& db, const string& schema_file) {
    try {
        ifstream file(schema_file);
        if (file) {
            json schema;
            file >> schema;
            db.schema_name = schema["name"];
            createDirectories(db, schema["structure"]);
            for (const auto& table : schema["structure"].items()) {
                addNode(db, table.key());
            }
        } else {
            throw runtime_error("Failed to open schema file.");
        }
    } catch (const exception& e) {
        cout << "Error: " << e.what() << endl;
    }
}

void updatePrimaryKey(dbase& db) {
    try {
        string pk_filename = db.schema_name + "/table_pk_sequence.txt";
        ifstream pk_file(pk_filename);
        if (pk_file) {
            pk_file >> db.current_pk;
        } else {
            db.current_pk = 0;
        }
        pk_file.close();

        db.current_pk++;

        ofstream pk_file_out(pk_filename);
        if (pk_file_out) {
            pk_file_out << db.current_pk << "\nlocked";
        } else {
            throw runtime_error("Failed to open file for updating: " + pk_filename);
        }
    } catch (const exception& e) {
        cout << "Error: " << e.what() << endl;
    }
}

Node* findNode(dbase& db, const string& table_name) {
    Node* current = db.head;
    while (current) {
        if (current->name == table_name) {
            return current;
        }
        current = current->next;
    }
    return nullptr;
}

size_t getColumnCount(dbase& db, const string& table) {
    Node* table_node = findNode(db, table);
    if (table_node) {
        string filename = db.schema_name + "/" + table + "/1.csv";
        ifstream file(filename);
        if (file) {
            string header;
            if (getline(file, header)) {
                size_t comma_count = std::count(header.begin(), header.end(), ',');
                return comma_count + 1; // Количество колонок = количество запятых + 1
            }
        }
    }
    return 0; 
}

void load(dbase& db) {
    Node* current = db.head;
    while (current) {
        try {
            string filename = db.schema_name + "/" + current->name + "/1.csv"; 
            ifstream file(filename);
            if (file) {
                string line;
                bool is_header = true;
                while (getline(file, line)) {
                    line.erase(0, line.find_first_not_of(" \t"));
                    line.erase(line.find_last_not_of(" \t") + 1);

                    if (is_header) {
                        is_header = false;
                        continue;
                    }

                    istringstream iss(line);
                    Array fields;
                    string field;

                    while (getline(iss, field, ',')) {
                        field.erase(0, field.find_first_not_of(" \t"));
                        field.erase(field.find_last_not_of(" \t") + 1);
                        if (!field.empty()) {
                            fields.addEnd(field);
                        }
                    }

                    if (fields.getSize() == 4) {
                        json entry;
                        entry["name"] = fields.get(0);
                        entry["age"] = fields.get(1);
                        entry["adress"] = fields.get(2);
                        entry["number"] = fields.get(3);

                        current->data.addEnd(entry.dump());
                    } 
                }
            } else {
                throw runtime_error("Failed to open data file: " + filename);
            }
        } catch (const exception& e) {
            cout << "Error: " << e.what() << endl;
        }
        current = current->next;
    }
}

bool applyAndFilters(const json& entry, const MyPair<string, string> filters[], int filter_count) {
    for (int i = 0; i < filter_count; ++i) {
        const string& filter_column = filters[i].first;
        const string& filter_value = filters[i].second;

        if (!entry.contains(filter_column) || entry[filter_column].get<string>() != filter_value) {
            return false;
        }
    }
    return true;
}

bool applyOrFilters(const json& entry, const MyPair<string, string> filters[], int filter_count) {
    for (int i = 0; i < filter_count; ++i) {
        const string& filter_column = filters[i].first;
        const string& filter_value = filters[i].second;

        if (entry.contains(filter_column) && entry[filter_column].get<string>() == filter_value) {
            return true;
        }
    }
    return false;
}

void select(dbase& db, const string& column, const string& table, const MyPair<string, string> filters[], int filter_count, const string& filter_type) {
    Node* table_node = findNode(db, table);
    if (table_node) {
        bool data_found = false;
        for (size_t i = 0; i < table_node->data.getSize(); ++i) {
            json entry = json::parse(table_node->data.get(i));
            bool valid_row = false;

            if (filter_type == "AND") {
                valid_row = applyAndFilters(entry, filters, filter_count);
            } else if (filter_type == "OR") {
                valid_row = applyOrFilters(entry, filters, filter_count);
            }

            if (valid_row) {
                data_found = true;
                if (column == "all") {
                    cout << "Data from table " << table << ": ";
                    cout << entry["name"].get<string>() << "| "<< entry["age"].get<int>() << "| "<< entry["adress"].get<string>() << "| "<< entry["number"].get<string>() << endl;
                } else if (entry.contains(column)) {
                    cout << "Data from " << column << " in " << table << ": ";
                    cout << entry[column].get<string>() << endl;
                } else {
                    cout << "Error: Column '" << column << "' does not exist in table '" << table << "'." << endl;
                }
            }
        }
        if (!data_found) {
            cout << "No such data found in the table." << endl;
        }
    } else {
        cout << "Table not found: " << table << endl;
    }
}

void saveSingleEntryToCSV(dbase& db, const string& table, const json& entry) {
    try {
        string filename = db.schema_name + "/" + table + "/1.csv"; 
        ofstream file(filename, ios::app);
        if (file) {
            if (entry.contains("name") && entry.contains("age")) {
                file << setw(10) << left << entry["name"].get<string>() << ", "
                     << setw(10) << left << entry["age"];

                if (getColumnCount(db, table) > 2) {
                    file << ", " << setw(10) << left << entry["adress"].get<string>() << ", ";
                    file << setw(10) << left << entry["number"].get<string>();
                }

                file << "\n"; 
                cout << "Data successfully saved for: " << entry.dump() << endl;
            } else {
                throw runtime_error("Entry must contain 'name' and 'age'.");
            }
        } else {
            throw runtime_error("Failed to open data file for saving: " + filename);
        }
    } catch (const exception& e) {
        cout << "Error: " << e.what() << endl;
    }
}

void insert(dbase& db, const string& table, json entry) {
    Node* table_node = findNode(db, table);
    if (table_node) {
        updatePrimaryKey(db); 
        entry["id"] = db.current_pk; 

        table_node->data.addEnd(entry.dump());
        cout << "Inserted: " << entry.dump() << endl;

        saveSingleEntryToCSV(db, table, entry);
    } else {
        cout << "Table not found: " << table << endl;
    }
}

void rewriteCSV(dbase& db, const string& table) {
    try {
        string filename = db.schema_name + "/" + table + "/1.csv"; 
        ofstream file(filename); 

        if (file) {
            Node* table_node = findNode(db, table);
            if (table_node) {
                json columns = {"name", "age", "adress", "number"};

                for (const auto& column : columns) {
                    file << setw(10) << left << column.get<string>() << (column != columns.back() ? ", " : "");
                }
                file << "\n"; 

                for (size_t i = 0; i < table_node->data.getSize(); ++i) {
                    json entry = json::parse(table_node->data.get(i));
                    for (const auto& column : columns) {
                        file << setw(10) << left << entry[column.get<string>()].get<string>() << (column != columns.back() ? ", " : "");
                    }
                    file << "\n"; 
                }
            }
            file.close();
        } else {
            throw runtime_error("Failed to open data file for rewriting: " + filename);
        }
    } catch (const exception& e) {
        cout << "Error: " << e.what() << endl;
    }
}

void deleteRow(dbase& db, const string& column, const string& value, const string& table) {
    Node* table_node = findNode(db, table);
    if (table_node) {
        Array new_data;
        bool found = false;

        for (size_t i = 0; i < table_node->data.getSize(); ++i) {
            json entry = json::parse(table_node->data.get(i));
            if (entry.contains(column) && entry[column].get<string>() == value) {
                found = true;
                cout << "Deleted row: " << entry.dump() << endl;
            } else {
                new_data.addEnd(table_node->data.get(i));
            }
        }

        if (found) {
            table_node->data = new_data;
            rewriteCSV(db, table);
        } else {
            cout << "Row with " << column << " = " << value << " not found in table " << table << endl;
        }
    } else {
        cout << "Table not found: " << table << endl;
    }
}

void executeQuery(dbase& db, const string& query) {
    istringstream iss(query);
    string action;
    iss >> action;

    try {
        if (action == "INSERT") {
            string table;
            iss >> table;

            Array args;
            string arg;

            while (iss >> arg) {
                args.addEnd(arg);
            }

            size_t expected_arg_count = 4; // Для простоты, можно использовать это значение или вычислить через getColumnCount()
            if (args.getSize() > expected_arg_count) {
                cout << "Error: Too many arguments (" << args.getSize() << ") for INSERT command." << endl;
                return;
            }

            json entry = {
                {"name", args.get(0)},
                {"age", stoi(args.get(1))}
            };

            if (args.getSize() > 2) {
                entry["adress"] = args.get(2);
            } else {
                entry["adress"] = ""; // Значение по умолчанию
            }

            if (args.getSize() > 3) {
                entry["number"] = args.get(3);
            } else {
                entry["number"] = ""; // Значение по умолчанию
            }

            insert(db, table, entry);
        } else if (action == "SELECT") {
            string column, from, table, filter_type = "AND";
            iss >> column >> from >> table;
            if (from == "FROM") {
                MyPair<string, string> filters[10];
                int filter_count = 0;
                string filter_part;

                if (iss >> filter_part && filter_part == "WHERE") {
                    string filter_column;
                    string filter_value;
                    while (iss >> filter_column) {
                        string condition;
                        iss >> condition;
                        iss >> filter_value;
                        filters[filter_count++] = MyPair<string, string>(filter_column, filter_value);

                        string connector;
                        if (iss >> connector) {
                            if (connector == "OR") {
                                filter_type = "OR";
                            } else if (connector != "AND") {
                                break; // Прекратить чтение фильтров при некорректном соединителе
                            }
                        }
                    }
                }
                select(db, column, table, filters, filter_count, filter_type);
            } else {
                throw runtime_error("Invalid query format.");
            }
        } else if (action == "DELETE") {
            string column, from, value, table;
            iss >> from >> table >> column >> value;
            deleteRow(db, column, value, table);
        } else {
            throw runtime_error("Unknown command: " + query);
        }
    } catch (const exception& e) {
        cout << "Error: " << e.what() << endl;
    }
}
