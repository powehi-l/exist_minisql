#include "executor/execute_engine.h"
#include "glog/logging.h"

ExecuteEngine::ExecuteEngine() {

}

dberr_t ExecuteEngine::Execute(pSyntaxNode ast, ExecuteContext *context) {
  if (ast == nullptr) {
    return DB_FAILED;
  }
  switch (ast->type_) {
    case kNodeCreateDB:
      return ExecuteCreateDatabase(ast, context);
    case kNodeDropDB:
      return ExecuteDropDatabase(ast, context);
    case kNodeShowDB:
      return ExecuteShowDatabases(ast, context);
    case kNodeUseDB:
      return ExecuteUseDatabase(ast, context);
    case kNodeShowTables:
      return ExecuteShowTables(ast, context);
    case kNodeCreateTable:
      return ExecuteCreateTable(ast, context);
    case kNodeDropTable:
      return ExecuteDropTable(ast, context);
    case kNodeShowIndexes:
      return ExecuteShowIndexes(ast, context);
    case kNodeCreateIndex:
      return ExecuteCreateIndex(ast, context);
    case kNodeDropIndex:
      return ExecuteDropIndex(ast, context);
    case kNodeSelect:
      return ExecuteSelect(ast, context);
    case kNodeInsert:
      return ExecuteInsert(ast, context);
    case kNodeDelete:
      return ExecuteDelete(ast, context);
    case kNodeUpdate:
      return ExecuteUpdate(ast, context);
    case kNodeTrxBegin:
      return ExecuteTrxBegin(ast, context);
    case kNodeTrxCommit:
      return ExecuteTrxCommit(ast, context);
    case kNodeTrxRollback:
      return ExecuteTrxRollback(ast, context);
    case kNodeExecFile:
      return ExecuteExecfile(ast, context);
    case kNodeQuit:
      return ExecuteQuit(ast, context);
    default:
      break;
  }
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteCreateDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateDatabase" << std::endl;
#endif

    if (dbs_[ast->child_->val_]){
        cout << "Error : Can't create database '" << ast->child_->val_ << "'; database exists";
        return DB_FAILED;
    }
    auto *db = new DBStorageEngine(ast->child_->val_, true);
    dbs_[ast->child_->val_] = db;
    cout << "Success!";
    return DB_SUCCESS;
//  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteDropDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropDatabase" << std::endl;
#endif
    if (!dbs_[ast->child_->val_]){
        cout << "Error : Can't drop database '" << ast->child_->val_ << "'; database doesn't exist";
        return DB_FAILED;
    }
    delete dbs_[ast->child_->val_];
    dbs_.erase(ast->child_->val_);
    cout << "Success!";
    return DB_SUCCESS;
//  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteShowDatabases(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowDatabases" << std::endl;
#endif
    if (dbs_.empty()){
        cout << "Empty!";
        return DB_SUCCESS;
    }
    cout << "+--------------------+" << endl;
    cout << " Database" << endl;
    cout << "+--------------------+" << endl;
    for (auto & db : dbs_)
        cout << " " << db.first << endl;
    cout << "+--------------------+" << endl;
    return DB_SUCCESS;
//  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteUseDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteUseDatabase" << std::endl;
#endif
    if (!dbs_[ast->child_->val_]){
        cout << "Error : Unknown database '" << ast->child_->val_ << "'";
        return DB_FAILED;
    }
    current_db_ = ast->child_->val_;
    current_db = dbs_[current_db_];
    cout << "Success!";
    return DB_SUCCESS;
//  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteShowTables(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowTables" << std::endl;
#endif
    if (!current_db) {
        cout << "Error : No database selected";
        return DB_FAILED;
    }
    vector<TableInfo *> tables;
    current_db->catalog_mgr_->GetTables(tables);
    if (tables.empty()) {
        cout << "Empty set";
        return DB_SUCCESS;
    }
    cout << "+--------------------+" << endl;
    cout << " Tables_in_" << current_db_ << endl;
    cout << "+--------------------+" << endl;
    for(auto t = tables.begin(); t < tables.end(); t++)
        cout << " " << (*t)->GetTableName() << endl;
    cout << "+--------------------+" << endl;
    return DB_SUCCESS;
//  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteCreateTable(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateTable" << std::endl;
#endif
    if (!current_db) {
        cout << "Error : No database selected";
        return DB_FAILED;
    }
    pSyntaxNode pointer = ast->child_;
    string new_table_name = pointer->val_;
    //put in ColumnDefinition[0]
    pointer = pointer->next_->child_;
    vector<Column *> new_table_column;
    while(pointer != nullptr && pointer->type_ == kNodeColumnDefinition){
        bool is_unique = false;
        if ((string)pointer->val_ == "unique")
            is_unique = true;
        string new_column_name = pointer->child_->val_;
        string new_column_type = pointer->child_->next_->val_;
        int index = 0;
        Column *new_column;
        if(new_column_type == "int")
            new_column = new Column(new_column_name, kTypeInt, index, true, is_unique);
        else if(new_column_type == "float")
            new_column = new Column(new_column_name, kTypeFloat, index, true, is_unique);
        else if(new_column_type == "char"){
            float l = atof(pointer->child_->next_->child_->val_);
            if (l <= 0){
                cout << "Error : String length can't be negative!";
                return DB_FAILED;
            }
            if (l - (float)((int)l) >= 1e-6) {
                cout << "Error : String length can't be decimal!";
                return DB_FAILED;
            }
            auto length = (uint32_t)l;
            new_column = new Column(new_column_name, kTypeChar, length, index, true, is_unique);
        }
        else{
            cout<<"Error : Unknown column type!"<<endl;
            return DB_FAILED;
        }
        new_table_column.push_back(new_column);
        pointer = pointer->next_;
        index++;
    }
    //schema
    auto *new_schema = new Schema(new_table_column);
    TableInfo *table_info = nullptr;
    dberr_t create_table =
            current_db->catalog_mgr_->CreateTable(new_table_name, new_schema,nullptr,table_info);
    if(create_table == DB_TABLE_ALREADY_EXIST){
        cout << "Error : Table Already Exist!";
        return create_table;
    }
    //primary key index
    if (pointer != nullptr && (string)pointer->val_ == "primary keys"){
        pointer = pointer->child_;
        vector<string> primary_keys;
        while(pointer != nullptr && pointer->type_ == kNodeIdentifier){
            string key_name = pointer->val_;
            primary_keys.push_back(key_name);
            pointer = pointer->next_;
        }
        IndexInfo *index_info = nullptr;
        string primary_key_index_name = new_table_name + "_primary_key";
        current_db->catalog_mgr_->CreateIndex(new_table_name, primary_key_index_name, primary_keys, nullptr, index_info);
    }
//    //unique index
//    for (auto & iterator : new_table_column){
//        if (iterator->IsUnique()){
//            string unique_index_name = new_table_name + "_" + iterator->GetName() + "_unique";
//            vector<string> unique_column_name = { iterator->GetName() };
//            IndexInfo *index_info = nullptr;
//            current_db->catalog_mgr_->CreateIndex(new_table_name,unique_index_name,unique_column_name,nullptr,index_info);
//        }
//    }
    cout << "Success!";
    return create_table;
//  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteDropTable(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropTable" << std::endl;
#endif
    dberr_t drop_table = current_db->catalog_mgr_->DropTable(ast->child_->val_);
    if(drop_table == DB_TABLE_NOT_EXIST)
        cout << "Error : Table Not Exist!";
    return drop_table;
    //return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteShowIndexes(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowIndexes" << std::endl;
#endif
  if (!current_db) {
    cout << "Error : No database selected";
    return DB_FAILED;
  }
//  auto mgr = current_db->catalog_mgr_;
//  cout << "+-------+-------------+------------+-----------+"<<endl;
//  cout << "| Table |"<< " Seq_in_index |" << " Column_name |" << " Index_type |";
//  std::vector<IndexInfo *>indexes;
//  mgr->GetTableIndexes()
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteCreateIndex(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateIndex" << std::endl;
#endif
  if (!current_db) {
    cout << "Error : No database selected";
    return DB_FAILED;
  }
  auto mgr = current_db->catalog_mgr_;

  auto pointer = ast->child_;
  std::string new_index = pointer->val_;
  pointer = pointer->next_;
  std::string new_index_table = pointer->val_;

//  TableInfo *cur_table;
//  mgr->GetTable(new_index_table, cur_table);
//  Schema* table_schema = cur_table->GetSchema();
//  MemHeap* cur_heap;
//  mgr->GetHeap(cur_heap);
  pointer = pointer->next_->child_;
  std::vector<std::string> index_child;
  while(pointer != nullptr && pointer->type_ == kNodeIdentifier) {
    index_child.push_back(pointer->val_);
    pointer = pointer->next_;
  }
  IndexInfo* index_info;
  mgr->CreateIndex(new_index_table, new_index, index_child, nullptr, index_info);
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteDropIndex(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropIndex" << std::endl;
#endif
  if (!current_db) {
    cout << "Error : No database selected";
    return DB_FAILED;
  }
  auto mgr = current_db->catalog_mgr_;
  auto pointer = ast->child_;
  std::string drop_index = pointer->val_;

  mgr->DropIndex(drop_index);
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteSelect(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteSelect" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteInsert(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteInsert" << std::endl;
#endif
    pSyntaxNode pointer = ast->child_;
    string table_name = pointer->val_;
    TableInfo* table_info = nullptr;
    dberr_t get_table = current_db->catalog_mgr_->GetTable(table_name, table_info);
    if (get_table == DB_TABLE_NOT_EXIST){
        cout << "Error : Table not exist! Affects 0 record!";
        return DB_FAILED;
    }
    vector<Field> new_fields;
    pointer = pointer->next_->child_;
    int index = table_info->GetSchema()->GetColumnCount();
    for (auto i = 0; i < index; i++){
        TypeId index_type = table_info->GetSchema()->GetColumn(i)->GetType();
        if(pointer->val_ == nullptr){
            Field new_field(index_type);
            new_fields.push_back(new_field);
            pointer = pointer->next_;
            continue;
        }
//        if (pointer == nullptr){
//            for (auto j = i; j < index; j++){
//                Field new_field(table_info->GetSchema()->GetColumn(j)->GetType());
//                fields.push_back(new_field);
//            }
//            break;
//        }
        if (index_type == kTypeInt){
            int int_value = atoi(pointer->val_);
            Field new_field (kTypeInt, int_value);
            new_fields.push_back(new_field);
        }
        else if(index_type==kTypeFloat){
            float float_value = atof(pointer->val_);
            Field new_field (kTypeFloat, float_value);
            new_fields.push_back(new_field);
        }
        else {
            char *char_value = new char[strlen(pointer->val_) + 1];
            strcpy(char_value, pointer->val_);
            Field new_field (kTypeChar, char_value, strlen(pointer->val_), true);
            new_fields.push_back(new_field);
        }
        pointer = pointer->next_;
    }
//    if (pointer != nullptr){
//        cout << "Error : Column Count doesn't match!";
//        return DB_FAILED;
//    }
    Row new_row(new_fields);
    TableHeap* table_heap = table_info->GetTableHeap();
    bool insert = table_heap->InsertTuple(new_row,nullptr);
    if(!insert){
        cout<<"Error : Insert failed! Affects 0 record!"<<endl;
        return DB_FAILED;
    }
    vector<IndexInfo *> indexes;
    current_db->catalog_mgr_->GetTableIndexes(table_name, indexes);
    for (auto iter = indexes.begin(); iter != indexes.end() ; iter++) {
        //未完成
    }
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteDelete(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDelete" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteUpdate(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteUpdate" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteTrxBegin(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxBegin" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteTrxCommit(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxCommit" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteTrxRollback(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxRollback" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteExecfile(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteExecfile" << std::endl;
#endif

  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteQuit(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteQuit" << std::endl;
#endif
  ASSERT(ast->type_ == kNodeQuit, "Unexpected node type.");
  context->flag_quit_ = true;
  return DB_SUCCESS;
}
