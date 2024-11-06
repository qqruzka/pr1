#include "dbase.h"

// Конструктор узла
Node::Node(const string& name) : name(name), next(nullptr) {}

// Конструктор базы данных
dbase::dbase() : head(nullptr), current_pk(0) {}

// Деструктор базы данных
dbase::~dbase() {
    while (head) {
        Node* temp = head;
        head = head->next;
        delete temp;
    }
}

// Блокировка первичного ключа
void dbase::lockPrimaryKey() {
    try {
        string pk_filename = schema_name + "\\table_pk_sequence.txt";
        ofstream pk_file(pk_filename);
        if (pk_file) {
            pk_file << current_pk << "\nlocked";
        } else {
            throw runtime_error("Failed to lock primary key file: " + pk_filename);
        }
    } catch (const exception& e) {
        cout << "Error: " << e.what() << endl;
    }
}

// Разблокировка первичного ключа
void dbase::unlockPrimaryKey() {
    try {
        string pk_filename = schema_name + "\\table_pk_sequence.txt";
        ofstream pk_file(pk_filename);
        if (pk_file) {
            pk_file << current_pk << "\nunlocked";
        } else {
            throw runtime_error("Failed to unlock primary key file: " + pk_filename);
        }
    } catch (const exception& e) {
        cout << "Error: " << e.what() << endl;
    }
}

// Загрузка схемы
void dbase::loadSchema(const string& schema_file) {
    try {
        ifstream file(schema_file);
        if (file) {
            json schema;
            file >> schema;
            schema_name = schema["name"];
            createDirectories(schema["structure"]);
            for (const auto& table : schema["structure"].items()) {
                addNode(table.key());
            }
        } else {
            throw runtime_error("Failed to open schema file.");
        }
    } catch (const exception& e) {
        cout << "Error: " << e.what() << endl;
    }
}

// Инициализация первичного ключа
void dbase::initializePrimaryKey() {
    try {
        string pk_filename = schema_name + "\\table_pk_sequence.txt";
        ifstream pk_file(pk_filename);
        
        if (pk_file) {
            pk_file >> current_pk;
        } else {
            current_pk = 0;
            ofstream pk_file_out(pk_filename);
            if (pk_file_out) {
                pk_file_out << current_pk << "\nunlocked";
            } else {
                throw runtime_error("Failed to create file: " + pk_filename);
            }
        }
    } catch (const exception& e) {
        cout << "Error: " << e.what() << endl;
    }
}

// Обновление первичного ключа
void dbase::updatePrimaryKey() {
    try {
        string pk_filename = schema_name + "\\table_pk_sequence.txt";

        ifstream pk_file(pk_filename);
        if (pk_file) {
            pk_file >> current_pk;
        } else {
            current_pk = 0;
        }
        pk_file.close();

        current_pk++;

        ofstream pk_file_out(pk_filename);
        if (pk_file_out) {
            pk_file_out << current_pk << "\nlocked";
        } else {
            throw runtime_error("Failed to open file for updating: " + pk_filename);
        }
    } catch (const exception& e) {
        cout << "Error: " << e.what() << endl;
    }
}

// Получение количества колонок в таблице
size_t dbase::getColumnCount(const string& table) {
    Node* table_node = findNode(table);
    if (table_node) {
        string filename = schema_name + "\\" + table + "\\1.csv";
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

// Создание директорий для таблиц
void dbase::createDirectories(const json& structure) {
    try {
        _mkdir(schema_name.c_str());

        for (const auto& table : structure.items()) {
            string table_name = table.key();
            string table_path = schema_name + "\\" + table_name;

            _mkdir(table_path.c_str());
            filename = table_path + "\\1.csv";

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

            initializePrimaryKey();
        }
    } catch (const exception& e) {
        cout << "Error: " << e.what() << endl;
    }
}

// Добавление узла в список таблиц
void dbase::addNode(const string& table_name) {
    Node* new_node = new Node(table_name);
    new_node->next = head;
    head = new_node;
}

// Поиск узла по имени таблицы
Node* dbase::findNode(const string& table_name) {
    Node* current = head;
    while (current) {
        if (current->name == table_name) {
            return current;
        }
        current = current->next;
    }
    return nullptr;
}

// Загрузка данных из CSV файлов
void dbase::load() {
    Node* current = head;
    while (current) {
        try {
            filename = schema_name + "\\" + current->name + "\\1.csv"; 
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

// Применение фильтров AND
bool dbase::applyAndFilters(const json& entry, const pair<string, string> filters[], int filter_count) {
    for (int i = 0; i < filter_count; ++i) {
        const string& filter_column = filters[i].first;
        const string& filter_value = filters[i].second;

        if (!entry.contains(filter_column) || entry[filter_column].get<string>() != filter_value) {
            return false;
        }
    }
    return true;
}

// Применение фильтров OR
bool dbase::applyOrFilters(const json& entry, const pair<string, string> filters[], int filter_count) {
    for (int i = 0; i < filter_count; ++i) {
        const string& filter_column = filters[i].first;
        const string& filter_value = filters[i].second;

        if (entry.contains(filter_column) && entry[filter_column].get<string>() == filter_value) {
            return true;
        }
    }
    return false;
}

// Выбор данных из таблицы
void dbase::select(const string& column, const string& table, const pair<string, string> filters[], int filter_count, const string& filter_type) {
    Node* table_node = findNode(table);
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
                    cout << entry["name"].get<string>() << ", "
                         << entry["age"].get<int>() << ", "
                         << entry["adress"].get<string>() << ", "
                         << entry["number"].get<string>() << endl;
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

// Сохранение одной записи в CSV
void dbase::saveSingleEntryToCSV(const string& table, const json& entry) {
    try {
        string filename = schema_name + "\\" + table + "\\1.csv"; 
        ofstream file(filename, ios::app);
        if (file) {
            if (entry.contains("name") && entry.contains("age")) {
                file << setw(10) << left << entry["name"].get<string>() << ", "
                     << setw(10) << left << entry["age"];

                if (getColumnCount(table) > 2) {
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

// Вставка записи в таблицу
void dbase::insert(const string& table, json entry) {
    Node* table_node = findNode(table);
    if (table_node) {
        updatePrimaryKey(); 
        entry["id"] = current_pk; 

        table_node->data.addEnd(entry.dump());
        cout << "Inserted: " << entry.dump() << endl;

        saveSingleEntryToCSV(table, entry);
    } else {
        cout << "Table not found: " << table << endl;
    }
}

// Удаление строки из таблицы
void dbase::deleteRow(const string& column, const string& value, const string& table) {
    Node* table_node = findNode(table);
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
            rewriteCSV(table);
        } else {
            cout << "Row with " << column << " = " << value << " not found in table " << table << endl;
        }
    } else {
        cout << "Table not found: " << table << endl;
    }
}

// Перезапись CSV файла
void dbase::rewriteCSV(const string& table) {
    try {
        filename = schema_name + "\\" + table + "\\1.csv"; 
        ofstream file(filename); 

        if (file) {
            Node* table_node = findNode(table);
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

// Изменение функции executeQuery для поддержки фильтрации с AND и OR
void executeQuery(dbase& db, const string& query) {
    istringstream iss(query);
    string action;
    iss >> action;

    try {
        if (action == "INSERT") {
            string table;
            iss >> table;

            vector<string> args;
            string arg;

            while (iss >> arg) {
                args.push_back(arg);
            }

            size_t expected_arg_count = db.getColumnCount(table);
            if (args.size() > expected_arg_count) {
                cout << "Error: Too many arguments (" << args.size() << ") for INSERT command." << endl;
                return;
            } else if (args.size() < 2) {
                cout << "Error: Not enough arguments (" << args.size() << ") for INSERT command." << endl;
                return;
            }

            json entry = {
                {"name", args[0]},
                {"age", stoi(args[1])}
            };

            if (args.size() > 2) {
                entry["adress"] = args[2];
            } else {
                entry["adress"] = "";
            }

            if (args.size() > 3) {
                entry["number"] = args[3];
            } else {
                entry["number"] = "";
            }

            db.insert(table, entry);
        } else if (action == "SELECT") {
            string column, from, table, filter_type = "AND";
            iss >> column >> from >> table;
            if (from == "FROM") {
                pair<string, string> filters[10];
                int filter_count = 0;
                string filter_part;

                if (iss >> filter_part && filter_part == "WHERE") {
                    string filter_column, filter_value;
                    while (iss >> filter_column) {
                        string condition;
                        iss >> condition;
                        iss >> filter_value;
                        filters[filter_count++] = make_pair(filter_column, filter_value);

                        string connector;
                        iss >> connector;
                        if (connector == "OR") {
                            filter_type = "OR";
                        } else if (connector != "AND") {
                            break;
                        }
                    }
                }
                db.select(column, table, filters, filter_count, filter_type);
            } else {
                throw runtime_error("Invalid query format.");
            }
        } else if (action == "DELETE") {
            string column, from, value, table;
            iss >> from >> table >> column >> value;
            db.deleteRow(column, value, table);
        } else {
            throw runtime_error("Unknown command: " + query);
        }
    } catch (const exception& e) {
        cout << "Error: " << e.what() << endl;
    }
}
