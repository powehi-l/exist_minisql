#include "executor/execute_engine.h"
#include "glog/logging.h"
extern "C" {
int yyparse(void);
#include "parser/minisql_lex.h"
#include "parser/parser.h"
}

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
        if (pointer->val_ != nullptr){
            string s = pointer->val_;
            is_unique = (s == "unique");
        }
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
//    cout << "test" << endl;
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

vector<Row*> rec_sel(pSyntaxNode sn, std::vector<Row*>& r, TableInfo* t, CatalogManager* c){
    if(sn == nullptr) return r;
    if(sn->type_ == kNodeConnector){

        vector<Row*> ans;
        if(strcmp(sn->val_,"and") == 0){
            auto r1 = rec_sel(sn->child_,r,t,c);
            ans = rec_sel(sn->child_->next_,r1,t,c);
            return ans;
        }
        else if(strcmp(sn->val_,"or") == 0){
            auto r1 = rec_sel(sn->child_,r,t,c);
            auto r2 = rec_sel(sn->child_->next_,r,t,c);
            for(uint32_t i=0;i<r1.size();i++){
                ans.push_back(r1[i]);
            }
            for(uint32_t i=0;i<r2.size();i++){
                int flag=1;
                for(uint32_t j=0;j<r1.size();j++){
                    int f=1;
                    for(uint32_t k=0;k<r1[i]->GetFieldCount();k++){
                        if(!r1[i]->GetField(k)->CompareEquals(*r2[j]->GetField(k))){
                            f=0;break;
                        }
                    }
                    if(f==1){
                        flag=0;//���ظ�
                        break;}
                }
                if(flag==1) ans.push_back(r2[i]);
            }
            return ans;
        }
    }
    if(sn->type_ == kNodeCompareOperator){
        string op = sn->val_;
        string col_name = sn->child_->val_;
        string val = sn->child_->next_->val_;
        uint32_t keymap;
        vector<Row*> ans;
        if(t->GetSchema()->GetColumnIndex(col_name, keymap)!=DB_SUCCESS){
            cout<<"column not found"<<endl;
            return ans;
        }
        const Column* key_col = t->GetSchema()->GetColumn(keymap);
        TypeId type =  key_col->GetType();

        if(op == "="){
            if(type==kTypeInt)
            {
                int valint = std::stoi(val);
                Field benchmk(type,int(valint));
                vector<Field> vect_benchmk;
                vect_benchmk.push_back(benchmk);

                vector <IndexInfo*> indexes;
                c->GetTableIndexes(t->GetTableName(),indexes);
                for(auto p=indexes.begin();p<indexes.end();p++){
                    if((*p)->GetIndexKeySchema()->GetColumnCount()==1){
                        if((*p)->GetIndexKeySchema()->GetColumns()[0]->GetName()==col_name){
                            // cout<<"--select using index--"<<endl;
                            Row tmp_row(vect_benchmk);
                            vector<RowId> result;
                            (*p)->GetIndex()->ScanKey(tmp_row,result,nullptr);
                            for(auto q:result){
                                if(q.GetPageId()<0) continue;
                                Row *tr = new Row(q);
                                t->GetTableHeap()->GetTuple(tr,nullptr);
                                ans.push_back(tr);
                            }
                            return ans;
                        }
                    }
                }
                for(uint32_t i=0;i<r.size();i++){
                    if(!r[i]->GetField(keymap)->CheckComparable(benchmk)){
                        cout<<"not comparable"<<endl;
                        return ans;
                    }
                    if(r[i]->GetField(keymap)->CompareEquals(benchmk)){
                        Row* tr = new Row(*r[i]);
                        ans.push_back(tr);
                    }
                }
            }

            else if(type==kTypeFloat)
            {
                float valfloat = std::stof(val);
                Field benchmk(type,float(valfloat));
                for(uint32_t i=0;i<r.size();i++){
                    if(!r[i]->GetField(keymap)->CheckComparable(benchmk)){
                        cout<<"not comparable"<<endl;
                        return ans;
                    }
                    if(r[i]->GetField(keymap)->CompareEquals(benchmk)){
                        Row* tr = new Row(*r[i]);
                        ans.push_back(tr);
                    }
                }
            }

            else if(type==kTypeChar){
                char *ch = new char[val.size()];
                strcpy(ch,val.c_str());
                Field benchmk = Field(TypeId::kTypeChar, const_cast<char *>(ch), val.size(), true);
                vector<Field> vect_benchmk;
                vect_benchmk.push_back(benchmk);

                vector <IndexInfo*> indexes;
                c->GetTableIndexes(t->GetTableName(),indexes);
                for(auto p=indexes.begin();p<indexes.end();p++){
                    if((*p)->GetIndexKeySchema()->GetColumnCount()==1){
                        if((*p)->GetIndexKeySchema()->GetColumns()[0]->GetName()==col_name){
                            clock_t s,e;
                            s=clock();
                            Row tmp_row(vect_benchmk);
                            vector<RowId> result;
                            (*p)->GetIndex()->ScanKey(tmp_row,result,nullptr);
                            e=clock();
                            cout<<"name index select takes "<<double(e-s)/CLOCKS_PER_SEC<<"s to Execute."<<endl;
                            for(auto q:result){
                                if(q.GetPageId()<0) continue;
                                Row *tr = new Row(q);
                                t->GetTableHeap()->GetTuple(tr,nullptr);
                                ans.push_back(tr);
                            }
                            return ans;
                        }
                    }
                }

                for(uint32_t i=0;i<r.size();i++){
                    const char* test = r[i]->GetField(keymap)->GetData();

                    if(strcmp(test,ch)==0){
                        vector<Field> f;
                        for(auto it:r[i]->GetFields()){
                            f.push_back(*it);
                        }
                        Row* tr = new Row(*r[i]);
                        ans.push_back(tr);
                    }
                }
            }
        }
        else if(op == "<"){
            if(type==kTypeInt)
            {
                int valint = std::stoi(val);
                Field benchmk(type,int(valint));
                for(uint32_t i=0;i<r.size();i++){
                    if(!r[i]->GetField(keymap)->CheckComparable(benchmk)){
                        cout<<"not comparable"<<endl;
                        return ans;
                    }
                    if(r[i]->GetField(keymap)->CompareLessThan(benchmk)){
                        Row* tr = new Row(*r[i]);
                        ans.push_back(tr);
                    }
                }
            }
            else if(type==kTypeFloat)
            {
                float valfloat = std::stof(val);
                Field benchmk(type,float(valfloat));
                for(uint32_t i=0;i<r.size();i++){
                    if(!r[i]->GetField(keymap)->CheckComparable(benchmk)){
                        cout<<"not comparable"<<endl;
                        return ans;
                    }
                    if(r[i]->GetField(keymap)->CompareLessThan(benchmk)){
                        Row* tr = new Row(*r[i]);
                        ans.push_back(tr);
                    }
                }
            }
            else if(type==kTypeChar){
                char* ch = new char[key_col->GetLength()+2];
                strcpy(ch,val.c_str());
                for(uint32_t i=0;i<r.size();i++){
                    const char* test = r[i]->GetField(keymap)->GetData();
                    if(strcmp(test,ch)<0){
                        vector<Field> f;
                        for(auto it:r[i]->GetFields()){
                            f.push_back(*it);
                        }
                        Row* tr = new Row(*r[i]);
                        ans.push_back(tr);
                    }
                }
            }
        }
        else if(op == ">"){
            if(type==kTypeInt)
            {
                int valint = std::stoi(val);
                Field benchmk(type,int(valint));
                for(uint32_t i=0;i<r.size();i++){
                    if(!r[i]->GetField(keymap)->CheckComparable(benchmk)){
                        cout<<"not comparable"<<endl;
                        return ans;
                    }
                    if(r[i]->GetField(keymap)->CompareGreaterThan(benchmk)){
                        Row* tr = new Row(*r[i]);
                        ans.push_back(tr);
                    }
                }
            }
            else if(type==kTypeFloat)
            {
                float valfloat = std::stof(val);
                Field benchmk(type,float(valfloat));
                for(uint32_t i=0;i<r.size();i++){
                    if(!r[i]->GetField(keymap)->CheckComparable(benchmk)){
                        cout<<"not comparable"<<endl;
                        return ans;
                    }
                    if(r[i]->GetField(keymap)->CompareGreaterThan(benchmk)){
                        Row* tr = new Row(*r[i]);
                        ans.push_back(tr);
                    }
                }
            }
            else if(type==kTypeChar){
                char* ch = new char[key_col->GetLength()];
                strcpy(ch,val.c_str());//�Ƚ�Ŀ�����ch��

                for(uint32_t i=0;i<r.size();i++){
                    const char* test = r[i]->GetField(keymap)->GetData();

                    if(strcmp(test,ch)>0){
                        vector<Field> f;
                        for(auto it:r[i]->GetFields()){
                            f.push_back(*it);
                        }
                        Row* tr = new Row(*r[i]);
                        ans.push_back(tr);
                    }
                }
            }
        }
        else if(op == "<="){
            if(type==kTypeInt)
            {
                int valint = std::stoi(val);
                Field benchmk(type,int(valint));
                for(uint32_t i=0;i<r.size();i++){
                    if(!r[i]->GetField(keymap)->CheckComparable(benchmk)){
                        cout<<"not comparable"<<endl;
                        return ans;
                    }
                    if(r[i]->GetField(keymap)->CompareLessThanEquals(benchmk)){
                        Row* tr = new Row(*r[i]);
                        ans.push_back(tr);
                    }
                }
            }
            else if(type==kTypeFloat)
            {
                float valfloat = std::stof(val);
                Field benchmk(type,float(valfloat));
                for(uint32_t i=0;i<r.size();i++){
                    if(!r[i]->GetField(keymap)->CheckComparable(benchmk)){
                        cout<<"not comparable"<<endl;
                        return ans;
                    }
                    if(r[i]->GetField(keymap)->CompareLessThanEquals(benchmk)){
                        Row* tr = new Row(*r[i]);
                        ans.push_back(tr);
                    }
                }
            }
            else if(type==kTypeChar){
                char* ch = new char[key_col->GetLength()];
                strcpy(ch,val.c_str());

                for(uint32_t i=0;i<r.size();i++){
                    const char* test = r[i]->GetField(keymap)->GetData();

                    if(strcmp(test,ch)<=0){
                        vector<Field> f;
                        for(auto it:r[i]->GetFields()){
                            f.push_back(*it);
                        }
                        Row* tr = new Row(*r[i]);
                        ans.push_back(tr);
                    }
                }
            }
        }
        else if(op == ">="){
            if(type==kTypeInt)
            {
                int valint = std::stoi(val);
                Field benchmk(type,int(valint));
                for(uint32_t i=0;i<r.size();i++){
                    if(!r[i]->GetField(keymap)->CheckComparable(benchmk)){
                        cout<<"not comparable"<<endl;
                        return ans;
                    }
                    if(r[i]->GetField(keymap)->CompareGreaterThanEquals(benchmk)){
                        Row* tr = new Row(*r[i]);
                        ans.push_back(tr);
                    }
                }
            }
            else if(type==kTypeFloat)
            {
                float valfloat = std::stof(val);
                Field benchmk(type,float(valfloat));
                for(uint32_t i=0;i<r.size();i++){
                    if(!r[i]->GetField(keymap)->CheckComparable(benchmk)){
                        cout<<"not comparable"<<endl;
                        return ans;
                    }
                    if(r[i]->GetField(keymap)->CompareGreaterThanEquals(benchmk)){
                        Row* tr = new Row(*r[i]);
                        ans.push_back(tr);
                    }
                }
            }
            else if(type==kTypeChar){
                char* ch = new char[key_col->GetLength()];
                strcpy(ch,val.c_str());

                for(uint32_t i=0;i<r.size();i++){
                    const char* test = r[i]->GetField(keymap)->GetData();

                    if(strcmp(test,ch)>=0){
                        vector<Field> f;
                        for(auto it:r[i]->GetFields()){
                            f.push_back(*it);
                        }
                        Row* tr = new Row(*r[i]);
                        ans.push_back(tr);
                    }
                }
            }
        }
        else if(op == "<>"){
            if(type==kTypeInt)
            {
                int valint = std::stoi(val);
                Field benchmk(type,int(valint));
                for(uint32_t i=0;i<r.size();i++){
                    if(!r[i]->GetField(keymap)->CheckComparable(benchmk)){
                        cout<<"not comparable"<<endl;
                        return ans;
                    }
                    if(r[i]->GetField(keymap)->CompareNotEquals(benchmk)){
                        Row* tr = new Row(*r[i]);
                        ans.push_back(tr);
                    }
                }
            }
            else if(type==kTypeFloat)
            {
                float valfloat = std::stof(val);
                Field benchmk(type,float(valfloat));
                for(uint32_t i=0;i<r.size();i++){
                    if(!r[i]->GetField(keymap)->CheckComparable(benchmk)){
                        cout<<"not comparable"<<endl;
                        return ans;
                    }
                    if(r[i]->GetField(keymap)->CompareNotEquals(benchmk)){
                        Row* tr = new Row(*r[i]);
                        ans.push_back(tr);
                    }
                }
            }
            else if(type==kTypeChar){
                char* ch = new char[key_col->GetLength()];
                strcpy(ch,val.c_str());

                for(uint32_t i=0;i<r.size();i++){
                    const char* test = r[i]->GetField(keymap)->GetData();

                    if(strcmp(test,ch)!=0){
                        vector<Field> f;
                        for(auto it:r[i]->GetFields()){
                            f.push_back(*it);
                        }
                        Row* tr = new Row(*r[i]);
                        ans.push_back(tr);
                    }
                }
            }
        }
        return ans;
    }
    return r;
}

dberr_t ExecuteEngine::ExecuteSelect(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteSelect" << std::endl;
#endif
    pSyntaxNode range = ast->child_;
    vector<uint32_t> columns;
    string table_name=range->next_->val_;
    TableInfo *tableinfo = nullptr;
    dberr_t GetRet = current_db->catalog_mgr_->GetTable(table_name, tableinfo);
    if (GetRet==DB_TABLE_NOT_EXIST){
        cout<<"Table Not Exist!"<<endl;
        return DB_FAILED;
    }
    if(range->type_ == kNodeAllColumns){
        // cout<<"select all"<<endl;
        for(uint32_t i=0;i<tableinfo->GetSchema()->GetColumnCount();i++)
            columns.push_back(i);
    }
    else if(range->type_ == kNodeColumnList){
        // vector<Column*> all_columns = tableinfo->GetSchema()->GetColumns();
        pSyntaxNode col = range->child_;
        while(col!=nullptr){
            uint32_t pos;
            if(tableinfo->GetSchema()->GetColumnIndex(col->val_,pos)==DB_SUCCESS){
                columns.push_back(pos);
            }
            else{
                cout<<"column not found"<<endl;
                return DB_FAILED;
            }
            col = col->next_;
        }
    }
    cout<<"--------------------"<<endl;
    //cout<<endl;
    for(auto i:columns){
        cout<<tableinfo->GetSchema()->GetColumn(i)->GetName()<<"   ";
    }
    cout<<endl;
    cout<<"--------------------"<<endl;
    if(range->next_->next_==nullptr)//û��ѡ������
    {
        int cnt=0;
        for(auto it=tableinfo->GetTableHeap()->Begin(nullptr);it!=tableinfo->GetTableHeap()->End();it++){
            for(uint32_t j=0;j<columns.size();j++){
                if(it->GetField(columns[j])->IsNull()){
                    cout<<"null";
                }
                else
                    it->GetField(columns[j])->fprint();
                cout<<"  ";

            }
            cout<<endl;
            cnt++;
        }
        cout<<"Select Success, Affects "<<cnt<<" Record!"<<endl;
        return DB_SUCCESS;
    }
    else if(range->next_->next_->type_ == kNodeConditions){
        pSyntaxNode cond = range->next_->next_->child_;
        vector<Row*> origin_rows;
        string op = cond->val_;
        if(cond->type_ == kNodeCompareOperator && op == "="){
            string col_name = cond->child_->val_;//column name
            string val = cond->child_->next_->val_;//compare value
            uint32_t keymap;
            if(tableinfo->GetSchema()->GetColumnIndex(col_name, keymap)!=DB_SUCCESS){
                cout<<"column not found"<<endl;
                return DB_SUCCESS;
            }
            const Column* key_col = tableinfo->GetSchema()->GetColumn(keymap);
            TypeId type =  key_col->GetType();
            if(type==kTypeInt)
            {
                int valint = std::stoi(val);
                Field benchmk(type,int(valint));
                vector<Field> vect_benchmk;
                vect_benchmk.push_back(benchmk);
                vector <IndexInfo*> indexes;

                current_db->catalog_mgr_->GetTableIndexes(tableinfo->GetTableName(),indexes);
                for(auto p=indexes.begin();p<indexes.end();p++){
                    if((*p)->GetIndexKeySchema()->GetColumnCount()==1){
                        if((*p)->GetIndexKeySchema()->GetColumns()[0]->GetName()==col_name){
                            cout<<"--select int using index--"<<endl;
                            Row tmp_row(vect_benchmk);
                            vector<RowId> result;
                            (*p)->GetIndex()->ScanKey(tmp_row,result,nullptr);
                            for(auto q:result){
                                if(q.GetPageId()<0) continue;
                                Row *tr = new Row(q);
                                tableinfo->GetTableHeap()->GetTuple(tr,nullptr);
                                for(uint32_t j=0;j<columns.size();j++){
                                    tr->GetField(columns[j])->fprint();
                                    cout<<"  ";
                                }
                                cout<<endl;
                            }
                            return DB_SUCCESS;
                        }
                    }
                }
            }
            else if(type==kTypeChar){
                char *ch = new char[val.size()];
                strcpy(ch,val.c_str());//input compare object
                Field benchmk = Field(TypeId::kTypeChar, const_cast<char *>(ch), val.size(), true);
                vector<Field> vect_benchmk;
                vect_benchmk.push_back(benchmk);
                vector <IndexInfo*> indexes;

                current_db->catalog_mgr_->GetTableIndexes(tableinfo->GetTableName(),indexes);
                for(auto p=indexes.begin();p<indexes.end();p++){
                    if((*p)->GetIndexKeySchema()->GetColumnCount()==1){
                        if((*p)->GetIndexKeySchema()->GetColumns()[0]->GetName()==col_name){
                            cout<<"--select using char* index--"<<endl;
                            Row tmp_row(vect_benchmk);
                            vector<RowId> result;
                            (*p)->GetIndex()->ScanKey(tmp_row,result,nullptr);
                            for(auto q:result){
                                if(q.GetPageId()<0) continue;
                                Row *tr = new Row(q);
                                tableinfo->GetTableHeap()->GetTuple(tr,nullptr);
                                for(uint32_t j=0;j<columns.size();j++){
                                    tr->GetField(columns[j])->fprint();
                                    cout<<"  ";
                                }
                                cout<<endl;
                            }
                            return DB_SUCCESS;
                        }
                    }
                }
            }
        }
        for(auto it=tableinfo->GetTableHeap()->Begin(nullptr);it!=tableinfo->GetTableHeap()->End();it++){
            Row* tp = new Row(*it);
            origin_rows.push_back(tp);
        }
        auto ptr_rows  = rec_sel(cond, *&origin_rows,tableinfo,current_db->catalog_mgr_);

        for(auto it=ptr_rows.begin();it!=ptr_rows.end();it++){
            for(uint32_t j=0;j<columns.size();j++){
                (*it)->GetField(columns[j])->fprint();
                cout<<"  ";
            }
            cout<<endl;
        }
        cout<<"Select Success, Affects "<<ptr_rows.size()<<" Record!"<<endl;
    }

    return DB_SUCCESS;
//  return DB_FAILED;
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
//    new_fields[0].fprint();
//    new_fields[1].fprint();
//    new_fields[2].fprint();
    Row new_row(new_fields);
    TableHeap* table_heap = table_info->GetTableHeap();
    bool insert = table_heap->InsertTuple(new_row,nullptr);
    if(!insert){
        cout << "Error : Insert failed! Affects 0 record!";
        return DB_FAILED;
    }
    vector<IndexInfo *> indexes;
    current_db->catalog_mgr_->GetTableIndexes(table_name, indexes);
    for (auto iter = indexes.begin(); iter != indexes.end() ; iter++) {
        IndexSchema* index_schema = (*iter)->GetIndexKeySchema();
        vector<Field> index_fields;
        for(auto it:index_schema->GetColumns()){
            index_id_t tmp;
            if(table_info->GetSchema()->GetColumnIndex(it->GetName(),tmp)==DB_SUCCESS){
                index_fields.push_back(new_fields[tmp]);
            }
        }
        Row index_row(index_fields);
        dberr_t IsInsert=(*iter)->GetIndex()->InsertEntry(index_row,new_row.GetRowId(),nullptr);
        if(IsInsert == DB_FAILED){
            cout<<"Insert Failed, Affects 0 Record!"<<endl;
            for(auto q=indexes.begin();q!=iter;q++){
                IndexSchema* index_schema_already = (*q)->GetIndexKeySchema();
                vector<Field> index_fields_already;
                for(auto it:index_schema_already->GetColumns()){
                    index_id_t tmp_already;
                    if(table_info->GetSchema()->GetColumnIndex(it->GetName(),tmp_already)==DB_SUCCESS){
                        index_fields_already.push_back(new_fields[tmp_already]);
                    }
                }
                Row index_row_already(index_fields_already);
                (*q)->GetIndex()->RemoveEntry(index_row_already,new_row.GetRowId(),nullptr);
            }
            table_heap->MarkDelete(new_row.GetRowId(),nullptr);
            return IsInsert;
        }
    }
    cout<<"Insert Success, Affects 1 Record!"<<endl;
    return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteDelete(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDelete" << std::endl;
#endif
    string table_name=ast->child_->val_;
    TableInfo *tableinfo = nullptr;
    dberr_t GetRet = current_db->catalog_mgr_->GetTable(table_name, tableinfo);
    if (GetRet==DB_TABLE_NOT_EXIST){
        cout<<"Table Not Exist!"<<endl;
        return DB_FAILED;
    }
    TableHeap* tableheap=tableinfo->GetTableHeap();
    auto del = ast->child_;
    vector<Row*> tar;

    if(del->next_==nullptr){
        for(auto it=tableinfo->GetTableHeap()->Begin(nullptr);it!=tableinfo->GetTableHeap()->End();it++){
            Row* tp = new Row(*it);
            tar.push_back(tp);
        }
    }
    else{
        vector<Row*> origin_rows;
        for(auto it=tableinfo->GetTableHeap()->Begin(nullptr);it!=tableinfo->GetTableHeap()->End();it++){
            Row* tp = new Row(*it);
            origin_rows.push_back(tp);
        }
        tar  = rec_sel(del->next_->child_, *&origin_rows,tableinfo,current_db->catalog_mgr_);
    }
    for(auto it:tar){
        tableheap->ApplyDelete(it->GetRowId(),nullptr);
    }
    cout<<"Delete Success, Affects "<<tar.size()<<" Record!"<<endl;
    vector <IndexInfo*> indexes;
    current_db->catalog_mgr_->GetTableIndexes(table_name,indexes);
    // int cnt=0;
    for(auto p=indexes.begin();p!=indexes.end();p++){
        for(auto j:tar){
            vector<Field> index_fields;

            for(auto it:(*p)->GetIndexKeySchema()->GetColumns()){
                index_id_t tmp;
                if(tableinfo->GetSchema()->GetColumnIndex(it->GetName(),tmp)==DB_SUCCESS){
                    index_fields.push_back(*j->GetField(tmp));
                }
            }
            Row index_row(index_fields);
            (*p)->GetIndex()->RemoveEntry(index_row,j->GetRowId(),nullptr);
        }
    }
    return DB_SUCCESS;
//  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteUpdate(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteUpdate" << std::endl;
#endif
    string table_name=ast->child_->val_;
    TableInfo *tableinfo = nullptr;
    dberr_t GetRet = current_db->catalog_mgr_->GetTable(table_name, tableinfo);
    if (GetRet==DB_TABLE_NOT_EXIST){
        cout<<"Table Not Exist!"<<endl;
        return DB_FAILED;
    }
    TableHeap* tableheap=tableinfo->GetTableHeap();
    auto updates = ast->child_->next_;
    vector<Row*> tar;

    if(updates->next_==nullptr){
        for(auto it=tableinfo->GetTableHeap()->Begin(nullptr);it!=tableinfo->GetTableHeap()->End();it++){
            Row* tp = new Row(*it);
            tar.push_back(tp);
        }
    }
    else{
        vector<Row*> origin_rows;
        for(auto it=tableinfo->GetTableHeap()->Begin(nullptr);it!=tableinfo->GetTableHeap()->End();it++){
            Row* tp = new Row(*it);
            origin_rows.push_back(tp);
        }
        tar  = rec_sel(updates->next_->child_, *&origin_rows,tableinfo,current_db->catalog_mgr_);
    }
    updates = updates->child_;
    SyntaxNode* tmp_up = updates;
    int flag=1;
    vector <IndexInfo*> indexes;
    current_db->catalog_mgr_->GetTableIndexes(tableinfo->GetTableName(),indexes);
    while(tmp_up && tmp_up->type_ == kNodeUpdateValue){//ֱcheck if any index is being undated
        string col = tmp_up->child_->val_;
        for(auto p=indexes.begin();p<indexes.end();p++){
            if((*p)->GetIndexKeySchema()->GetColumnCount()==1){
                if((*p)->GetIndexKeySchema()->GetColumns()[0]->GetName()==col){
                    flag=0;
                    break;
                }
            }
        }
        tmp_up = tmp_up->next_;
    }
    if(flag==0){
        cout<<"index cannot be updated!!"<<endl;
        return DB_SUCCESS;
    }

    while(updates && updates->type_ == kNodeUpdateValue){
        string col = updates->child_->val_;
        string upval = updates->child_->next_->val_;
        uint32_t index;
        tableinfo->GetSchema()->GetColumnIndex(col,index);
        TypeId tid = tableinfo->GetSchema()->GetColumn(index)->GetType();
        if(tid == kTypeInt){
            Field* newval = new Field(kTypeInt,stoi(upval));
            for(auto it:tar){
                it->GetFields()[index] = newval;
            }
        }
        else if(tid == kTypeFloat){
            Field* newval = new Field(kTypeFloat,stof(upval));
            for(auto it:tar){
                it->GetFields()[index] = newval;
            }
        }
        else if(tid == kTypeChar){
            uint32_t len = tableinfo->GetSchema()->GetColumn(index)->GetLength();
            char* tc = new char[len];
            strcpy(tc,upval.c_str());
            Field* newval = new Field(kTypeChar,tc,len,true);
            for(auto it:tar){
                it->GetFields()[index] = newval;
            }
        }
        updates = updates->next_;
    }
    for(auto it:tar){
        tableheap->UpdateTuple(*it,it->GetRowId(),nullptr);
    }
    cout<<"Update Success, Affects "<<tar.size()<<" Record!"<<endl;
    return DB_SUCCESS;
//  return DB_FAILED;
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
    string file_name = ast->child_->val_;
    ifstream file_stream;
    file_stream.open(file_name.data());
    if (file_stream.is_open()){
        string s;
        while(getline(file_stream, s)){
            YY_BUFFER_STATE bp = yy_scan_string(s.c_str());
            if (bp == nullptr) {
                LOG(ERROR) << "Failed to create yy buffer state." << std::endl;
                exit(1);
            }
            yy_switch_to_buffer(bp);
            MinisqlParserInit();
            yyparse();
            ExecuteContext c;
            Execute(MinisqlGetParserRootNode(), &c);
        }
        return DB_SUCCESS;
    }
    else{
        cout << "Error : Failed opening file!";
        return DB_FAILED;
    }
}

dberr_t ExecuteEngine::ExecuteQuit(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteQuit" << std::endl;
#endif
  ASSERT(ast->type_ == kNodeQuit, "Unexpected node type.");
  context->flag_quit_ = true;
  return DB_SUCCESS;
}
