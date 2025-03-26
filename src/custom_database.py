import os
import polars as pl
import warnings
from datetime import datetime


def dict_from_cols(cols, data_row):
    data = {}
    for c in cols:
        if c in data_row.columns:
            data[c]= data_row[c].item()
        else:
            data[c] = None
    return data

class CsvDatabase():

    # Base class for API's to handle scraping & searching data. 
    # 092524: Checked CsvDatabase functions, 092724: Function testing and refinement

    def __init__(self, folder, schema = None, strict = False, expand_tbls = False, expand_fields = False):

        # folder: filepath for folder containing csv files that will be used to build a database
        # schema: dict of dict containing schema for each intended table
        # strict: for polars kwargs - coercing datatypes
        # expand_tbls: boolean - tables found in folder but not in schema should be included (if False, these tables are not loaded)
        # expand_fields: boolean - fields found in csv files but not in schema should be included (if False, these fields are not loaded)

        # Assumption: Anything in schema should be in database. If extra data exists, it can be loaded using expand bools. If schema outlines 
        # data not in the folder, create empty table for them.

        if os.path.isdir(folder) & os.path.exists(folder):
            all_csvs = [fn for fn in os.listdir(folder) if fn.endswith('.csv')]
        elif os.path.isfile(folder) & os.path.exists(folder):
            all_csvs = [folder]
        else:
            Exception(f"{folder} not found")

        self.init_dir = folder
        self.schema = schema
        self.strict = strict
        self.expand_fields = expand_fields
        self.database = {}
        self.reference = {}
        if not self.schema:
            self.schema = {}

        for fn in all_csvs:
            name, _ = os.path.splitext(fn)

            if schema is None:
                self.database[name] = None # CSV data is not explictly loaded until needed. Reference to filepath is sufficient to initialize object
                self.reference[name] = os.path.join(self.init_dir, fn)
                self.schema[name] = self.infer_schema(name)
            elif name in schema.keys():
                self.database[name] = None
                self.reference[name] = os.path.join(self.init_dir, fn)
                self.schema[name] = schema[name]
            elif expand_tbls:
                self.database[name] = None
                self.reference[name] = os.path.join(self.init_dir, fn)
                self.schema[name] = self.infer_schema(name)
        
        if schema:
            add_tbls = [x for x in schema.keys() if x not in self.schema.keys()]
            for tbl in add_tbls:
                self.create_tbl(name, schema = schema[tbl])
    
    def infer_schema(self, tbl):
        data = pl.read_csv(self.reference[tbl])
        return pl.Schema(data.schema).to_python()
    
    def match_schema(self, data, schema):

        # Match datatypes between data and schema. Data can be a polars dataframe or dict.

        casted = True
        if type(data) == dict:
            for k,v in data.items():
                if k in schema:
                    if type(v) == list:
                        casted_v = []
                        uncasted_v = []
                        for elem in v:
                            if type(v) != schema[k]:
                                try: 
                                    caster = schema[k]
                                    casted_v.append(caster(elem))
                                except Exception as e:
                                    casted_v.append(None)
                                    uncasted_v.append(elem)
                            else:
                                casted_v.append(elem)

                        data[k]= casted_v
                        if len(uncasted_v):
                            warnings.warn(f"Failed to cast {uncasted_v}")
                            casted = False

                    elif type(v) != schema[k]:
                        try:
                            caster = schema[k]
                            data[k] = caster(v)
                        except Exception as e:
                            warnings.warn(f"Failed to cast data '{v}' from {type(v)} to {caster}: {e}")
                            casted = False
                else:
                    warnings.warn(f'{k} not in schema')
                    casted = False
            return data, casted
        
        elif type(data) == pl.dataframe.frame.DataFrame:
            data = pl.DataFrame(data)
            for column, expected_dtype in schema.items():
                if column in data.columns:
                    actual_dtype = data[column].dtype
                    if actual_dtype != expected_dtype:
                        try:
                            data = data.with_columns(data[column].cast(expected_dtype))
                        except Exception as e:
                            warnings.warn(f"Failed to cast column '{column}' from {actual_dtype} to {expected_dtype}: {e}")
                            casted = False
            
            return data, casted
        else:
            warnings.warn('Only polars dataframe or dictionary can be used as input')
            return None, False

    def normalize_data_by_schema(self, schema, data):

        # Fits data to a given schema. 
        # If expand_fields, will also expand schema to match data.

        if type(data) == dict:
            if schema.keys() != data.keys():
                    schema_not_data = {x: None for x in schema.keys() if x not in data.keys()}
                    data.update(schema_not_data)
                    
                    if self.expand_fields:
                        data_not_schema = {k: type(v) for k,v in data.items() if k not in schema.keys()}
                        schema.update(data_not_schema)
                        # self.schema[tbl] = schema
                    else:
                        data = {k:v for k,v in data.items() if k in schema.keys()}
            data, _ = self.match_schema(data, schema)

        elif type(data) == pl.dataframe.frame.DataFrame:
            if list(schema.keys()) != data.columns:
                schema_not_data = {x: None for x in list(schema.keys()) if x not in data.columns}
                data = data.hstack(pl.DataFrame(data ={k: [None] * len(data) for k in schema_not_data.keys()}, schema = schema_not_data, strict = self.strict))

                if self.expand_fields:
                    data_not_schema = {k: v for k,v in dict(data.schema) if k not in schema.keys()}
                    schema.update(data_not_schema)
                    # self.schema[tbl] = schema
                else:
                    data = data.select(schema.keys())
        

        return data, schema

    def load_all(self):
        for tbl in self.schema.keys():
            self.load_table(tbl)
    
    def load_table(self, tbl):

        if tbl in self.database.keys():
            if self.database[tbl] is None:
                
                data = pl.read_csv(self.reference[tbl])
                schema = self.schema[tbl]
                if schema:
                    data, schema = self.normalize_data_by_schema(schema, data)
                
                try:
                    self.database[tbl] = pl.DataFrame(data = data, schema = schema, strict = self.strict)
                    self.schema[tbl] = schema
                    return True
                except Exception as e:
                    warnings.warn(f'Error: {e.args[0]}, {tbl} not created')
                    return False
            else:
                return True
        else:
            warnings.warn(f'{tbl} not found in {self.init_dir}')
            return False

    def get_tables(self):
        return list(self.database.keys())
    
    def get_fields(self, tbl):
        if self.schema and (tbl in self.schema.keys()):
            return list(self.schema[tbl].keys())
        elif self.load_table(tbl):
            return list(self.database[tbl].columns)
        else:
            return False

    def get_entries(self, tbl, key, null_search = False, as_dict = False):
        
        key, casted = self.match_schema(key, self.schema[tbl])

        if not casted:
            warnings.warn("Key doesn't match table schema. Try again")
            return False, None
        
        if self.load_table(tbl):
            
            if type(key) == pl.dataframe.frame.DataFrame:
                key = key.to_dict(as_series = False)

            if type(key) == dict:
                filter_expr = pl.lit(True)
                for col, val in key.items():
                    if col in self.get_fields(tbl):
                        if type(val) == list:
                            filter_expr &= (pl.col(col).is_in(val))
                        else:
                            filter_expr &= (pl.col(col) == val)
                    else:
                        warnings.warn(f'{col} not found in {tbl}')


            # elif type(key) == pl.dataframe.frame.DataFrame:

            #     filter_conditions = [(col, key[col][0]) for col in key.columns]
            #     filter_expr = [self.database[tbl][col] == value for col, value in filter_conditions]
            #     filter_expr = pl.all(filter_expr)

            if not as_dict:
                return True, self.database[tbl].filter(filter_expr)
            else:
                entries = self.database[tbl].filter(filter_expr)
                if len(entries) > 1:
                    return True, entries.to_dict(as_series = False)
                elif len(entries) == 1:
                    return True, {k:v[0] for k,v in entries.to_dict(as_series = False).items()}
                else:
                    return True, {k:None for k in entries.columns}

        else:
            return False, None

    def create_tbl(self, tbl, data = None, schema = None):

        if tbl in self.database.keys():
            warnings.warn(f"{tbl} already exists in database - nothing new created")
        else:
            tbl_fn = tbl+'.csv'
            
            if schema and data:
                data, schema = self.normalize_data_by_schema(schema, data)
            elif (schema is None) and (data is None):
                warnings.warn('Either data or schema must be defined')
                return False
            
            try:
                self.database[tbl] = pl.DataFrame(data = data, schema = schema, strict = self.strict)
                self.reference[tbl] = os.path.join(self.init_dir, tbl_fn)
                if schema is None:
                    schema = pl.Schema(self.database[tbl].schema).to_python()
                self.schema[tbl] = schema
                return True
            except Exception as e:
                print(f'Error: {e.args[0]}, {tbl} not created')
                return False
             
    def _create_entry(self, tbl, data): 

        # Private function to add a new entry to a table. Write entry has more functionality to check for 
        # existing entries that match some key criteria

        if not all(v is None for v in data.values()):
            schema = self.schema[tbl]
            if schema:
                data, schema = self.normalize_data_by_schema(schema, data)

            try:
                self.database[tbl] = pl.concat((self.database[tbl], pl.DataFrame(data = data, schema = schema, strict = self.strict)))
                self.schema[tbl] = schema
                return True
            except Exception as e:
                print(f'Error: {e.args[0]}, entry not added')
                return False
        else:
            print('Will not add an empty row')
            return False

    def write_entry(self, tbl, data, key = None, overwrite = False): 
        
        # Add data to a table. Key allows for checking for entry that might already
        # exist in table with keys. Returns boolean to indicate if entry was created
        
        if self.load_table(tbl):
            if key is None:
                return self._create_entry(tbl, data)
            else:
                key, casted = self.match_schema(key, self.schema[tbl])
                if casted:
                    matched, matches = self.get_entries(tbl, key)
                    # rows, _  = self.get_entries(tbl, key).shape # look to see if 
                    if (not matched) or matches.shape[0]== 0:
                        return self._create_entry(tbl, data)
                    elif matches.shape[0] == 1:
                        if overwrite:
                            self.delete_entries(tbl, key)
                            return self._create_entry(tbl, data)
                        else:
                            warnings.warn('Entry matching key already exists and overwrite is False')
                            return False
                    elif matches.shape[0] > 1:
                        warnings.warn('Multiple entries match key - refine key')
                        return False

        else:
            warnings.warn(f'{tbl} not in database')
            return False

    def update_field(self, tbl, key, data):
        # Used to update a single column/field of the entry - data is a tuple
        # Note all entries that match the key will be updated
        #   i.e. can be used to change all instances of 'rat' to "Rat"

        
        if len(data) > 2:
            warnings.warn(f'Update field is used to change a single column, data should be a two element tuple')
            return False
        
        key, casted = self.match_schema(key, self.schema[tbl])
        if casted:
            if self.load_table(tbl):
                if data[0] in self.get_fields(tbl):

                    if type(data[1]) != self.schema[tbl][data[0]]:
                        try:
                            caster =  self.schema[tbl][data[0]]
                            replace = caster(data[1])
                        except:
                            warnings.warn(f'Datatype does not match {self.schema[tbl][data[0]]}')
                            return False
                    else:
                        replace = data[1]


                    filter_expr = pl.lit(True)
                    for col, val in key.items():
                        if col in self.get_fields(tbl):
                            if type(val) == list:
                                filter_expr &= (pl.col(col).is_in(val))
                            else:
                                filter_expr &= (pl.col(col) == val)
                        else:
                            warnings.warn(f'{col} not found in {tbl}')
                            return False
                            
                    indices = self.database[tbl].select(pl.arg_where(filter_expr))['literal'].to_list()
                    if len(indices):
                        for i in indices:
                            self.database[tbl][i, data[0]] = replace
                        return True
                    else:
                        warnings.warn(f'No matching entry, refine key')
                        return False
                else:
                    warnings.warn(f'Column: {data[0]} not found in {tbl}')
                    return False
            else:
                return False
        else:
            return False
               
    def update_entry(self, tbl, key, data):
        # if you have a dictionary of data and only want to update columns that differ 
        # - should only be used for one to one relationship entries

        found, db_entry = self.get_entries(tbl, key, as_dict = True)

        if all([type(x) == list for x in db_entry.values()]):
            print('Multiple entries returned to update. Refine Key')
        else:
            updates  =  {}
            if data != db_entry:
                for k in data.keys():
                    if k in db_entry.keys():
                        if db_entry[k] != data[k]:
                            self.update_field(tbl, key, (k, data[k]))
                            updates[k] = (db_entry[k], data[k])
                    else:
                        print(f'{k} not in {tbl}')

            return updates
    
    def add_field(self, tbl, schema = None, data= None):
        if self.load_table(tbl):
            if (schema is None) and (data is None):
                return False
            elif data is None:
                name = schema[0]
                scheme = schema[1]
                if (type(scheme) == type) or (type(scheme) == pl.datatypes.classes.DataTypeClass):
                    self.database[tbl] = self.database[tbl].with_columns([pl.lit(None).cast(scheme).alias(name)])
                    self.schema[tbl].update({name:scheme})
                else:
                    warnings.warn('Schema must have a valid datatype (python or polars)')
                    return False
            else:
                name = data[0]
                dat = data[1]
                if type(dat) != list:
                    self.database[tbl] = self.database[tbl].with_columns(pl.lit(dat).alias(name))
                elif len(dat) == len(self.database[tbl]):
                    self.database[tbl] = self.database[tbl].with_columns(pl.Series(dat).alias(name))
                else:
                    warnings.warn('Data must be a single value or list matching height of table')
                    return False
                
                if schema is not None:
                    scheme = schema[1]
                    if (type(scheme) == type) or (type(scheme) == pl.datatypes.classes.DataTypeClass):
                        self.database[tbl] = self.database[tbl].with_columns(pl.col(name).cast(scheme))
                        self.schema[tbl].update({name:scheme})
                    else:
                        warnings.warn('Schema must have a valid datatype (python or polars)')
                        return False
                else:
                    self.schema[tbl].update({name: self.database[tbl].schema[name].to_python()}) 
                
            return True
        else:
            return False
    
    def delete_field(self, tbl, field):
        if self.load_table(tbl):
            if field in self.get_fields(tbl):
                self.database[tbl] = self.database[tbl].drop(pl.col(field))
            else:
                warnings.warn(f'{field} not in {tbl}')
        else:
            return False

    def delete_entries(self, tbl, key):

        if self.load_table(tbl):
            filter_expr = pl.lit(True)
            for col, val in key.items():
                if col in self.get_fields(tbl):
                    if type(val) == list:
                        filter_expr &= (pl.col(col).is_in(val))
                    else:
                        filter_expr &= (pl.col(col) == val)
                else:
                    print(f'{col} not found in {tbl}')

            deleted = self.database[tbl].filter(filter_expr)
            self.database[tbl] = self.database[tbl].filter(~filter_expr)
            return deleted
        else:
            print(f'{tbl} not found in {self.init_dir}')
            return None
    
    def save_tbl(self, tbl):
        if tbl in self.database.keys():
            self.database[tbl].write_csv(self.reference[tbl])
            return True
        else:
            return False

    def save_all_tbls(self):
        for k,v in self.reference.items():
            if self.database[k] is not None:
                self.database[k].write_csv(v)

    def delete_tbl(self, tbl):
        if self.load_table(tbl):
            self.database.pop(tbl)
            self.schema.pop(tbl)
            self.reference.pop(tbl)

class searchingAPI(CsvDatabase):

    def __init__(self, folder = None):
        if folder is None:
            folder = "/Database"

        schema = {
            
            'documents' : {'document_id': str, 'study_id': str, 'document_number': int, 'document_name':str, 'document_type':str, 'ext': str, 'directory': str, 'last_modified': datetime,
                                   'created': datetime, 'filepath': str, 'version': float},
            'studies': {'study_id': str, 'study_number': int, 'study_date': datetime, 
                        'client': str, 'species': str, 'sex': str,'description': str, 
                        'proposal_id': str, 'proposal_issue_date': datetime, 'proposal_latest_reissue': datetime, 
                        'report_id': str, 'report_issue_date': datetime, 'report_latest_reissue': datetime},
            'study_employees': {'study_id' : str, 'employee': str, 'role': str},
            'study_methods': {'study_id':str, 'method':str},
            'study_compounds' : {'study_id':str, 'compound': str},
            'study_strains': {'study_id': str, 'strain': str},
            'scraped_files': {'filepath': str, 'success': bool}

        }


        super().__init__(folder, schema = schema)
        self.create_filtered()
        
    def create_filtered(self):
        if self.load_table('studies'):
            
            self.filtered = self.database['studies']
            # s.map_elements(dateparser.parse, return_dtype = pl.datatypes.Datetime))
            # self.filtered = self.filtered.with_columns(self.filtered['study_date'].str.to_date('%m/%d/%y'))

    def get_possible_years(self):
        return self.filtered['study_date'].dt.year().min(), self.filtered['study_date'].dt.year().max()

    def get_possible_methods(self):
        if self.load_table('study_methods'):

            # method_count = self.database['document_methods'].group_by('method').len().sort('len', descending = True)
            method_count = self.database['study_methods'].unique('method')['method'].to_list()


            return sorted(method_count, key=str.casefold)

    def get_possible_compounds(self):
        if self.load_table('study_compounds'):

            # compound_count = self.database['document_compounds'].group_by('compound').len().sort('len', descending = True)
            compound_count = self.database['study_compounds'].unique('compound')['compound'].to_list()
            return sorted(compound_count, key = str.casefold)

    def get_possible_clients(self):
        if self.load_table('studies'):
        
            # client_count = self.database['documents'].group_by('client').len().sort('len', descending = True)
            client_count = self.database['studies'].unique('client')['client'].to_list()
            return sorted(client_count, key = str.casefold)
    
    def get_possible_strains(self, species= None):
        if self.load_table('study_strains'):
            strain_count =  self.database['study_strains'].unique('strain')['strain'].to_list()
            return sorted(strain_count, key = str.casefold)
            # if species is not None:
            #     return self.database['studies'].filter(pl.col('Species')== species)['strain'].to_list()
            # else:
            #     return self.database['strain']['Name'].to_list()

    def filter_by_method(self, method):
        if self.load_table('study_methods'):

            filtered_docs= self.database['study_methods'].filter(pl.col('method')== method).select('study_id')
            self.filtered = self.filtered.filter(pl.col('study_id').is_in(filtered_docs))
        
    def filter_by_compound(self, compound):
        if  self.load_table('study_compounds'):

            filtered_docs= self.database['study_compounds'].filter(pl.col('compound')== compound).select('study_id')
            self.filtered = self.filtered.filter(pl.col('study_id').is_in(filtered_docs))

    def filter_by_client(self, client):
        if self.load_table('studies'):

            filtered_docs= self.database['studies'].filter(pl.col('client')== client).select('study_id')
            self.filtered = self.filtered.filter(pl.col('study_id').is_in(filtered_docs))

    def filter_by_sex(self, sex):
        if self.load_table('studies'):

            if sex == 'males':
                sex = ['males', 'both']
            elif sex == 'females':
                sex = ['females', 'both']
            else:
                sex = [sex]

            filtered_docs= self.database['studies'].filter(pl.col('sex').is_in(sex)).select('study_id')
            self.filtered = self.filtered.filter(pl.col('study_id').is_in(filtered_docs))

    def filter_by_species(self, species):
        if self.load_table('studies'):

            if species == 'rat':
                species = ['rat', 'rat and mouse']
            elif species == 'mouse':
                species = ['mouse', 'rat and mouse']
            else:
                species = [species]

            filtered_docs= self.database['studies'].filter(pl.col('species').is_in(species)).select('study_id')
            self.filtered = self.filtered.filter(pl.col('study_id').is_in(filtered_docs))

    def filter_by_strain(self, strain):
        if self.load_table('study_strains'):

            filtered_docs= self.database['study_strains'].filter(pl.col('strain')== strain).select('study_id')
            self.filtered = self.filtered.filter(pl.col('study_id').is_in(filtered_docs))

    def filter_by_date(self, year):
        
        if type(year) == tuple:
            self.filtered = self.filtered.filter(pl.col('study_date').is_between(datetime(year[0], 1, 1), datetime(year[1], 12, 31))).sort(by = 'study_date')
        else:
            self.filtered = self.filtered.filter(pl.col('study_date').is_between(datetime(year, 1, 1), datetime(year, 12, 31))).sort(by = 'study_date')
    
    def get_matching_db(self):
        return self.filtered

    def get_matching_docs(self):
        ## change this  
        # if self.load_table('studies'):
        if self.load_table('documents'):
            matched_proposals = self.database['documents'].filter(pl.col('document_id').is_in(self.filtered['proposal_id'])).select(['study_id', 'filepath'])
            matched_reports = self.database['documents'].filter(pl.col('document_id').is_in(self.filtered['report_id'])).select(['study_id', 'filepath'])

            matched_data = matched_proposals.join(matched_reports, on = 'study_id',  how = 'outer', coalesce = True).rename({'filepath':'Proposals', 'filepath_right':'Reports'})
            return matched_data

    def check_filter_empty(self):
        if len(self.filtered) == 0:
            return True
        else:
            return False
        
class referenceAPI(CsvDatabase):

    def __init__(self, folder = None):
        if folder is None:
            folder = "/Reference"

        schema = {
            "strain": {
                "species": str,
                "strain": str,
                "Search 1": str,
                "Search 2": str,
                "Search 3": str,
            },
            "compounds": {"compound": str, "Alts": str},
            "methods": {
                "method_code": str,
                "method": str,
                "Search 1": str,
                "Search 2": str,
                "Search 3": str,
            },
            "clients": {
                "scrape": str,
                "alt 1": str,
                "alt 2": str,
                "client_code": str,
                "client": str,
                "Search 1": str,
                "Search 2": str,
                "Search 3": str,
                "Search 4": str,
                "Search 5": str,
            },
            "employees": {
                "status": str,
                "employee": str,
                "Search 1": str,
                "Search 2": str,
            },
        }
        super().__init__(folder, schema = schema)
    
    def get_possible_models(self):
        if self.load_table('methods'):
            possible_models = self.database['methods'].drop_nulls('method_code').unique('method')['method'].to_list()
            possible_models.sort()
        
            return possible_models

    def get_possible_strains(self):
        if self.load_table('strain'):
            possible_strains = self.database['strain'].unique('strain')['strain'].to_list()
            possible_strains.sort()

            return possible_strains
        
    def get_possible_employees(self, status = 'current'):
        if self.load_table('employees'):
            possible_employees = self.database['employees'].filter(pl.col('status') == status)['employee'].to_list()
            possible_employees.sort()

            return possible_employees

    def get_client_code(self, client):
        client_code = None
        found, client_match = self.get_entries(tbl = 'clients', key = {'client': client})
        if found:
            if len(client_match):
                codes = client_match['client_code'].unique().to_list()
                if len(codes) > 1:
                    print('multiple codes matched one client, whats up with that')
                    client_code = codes[0]
                elif len(codes) == 1:
                    client_code = codes[0]
            # else:
                # print(f'no client matched {client}')
        
        return client_code

    def get_model_code(self, model):
        model_code= None
        found, model_match = self.get_entries('methods', {'method': model})
        if found:
            if len(model_match):
                matching_codes = model_match['method_code'].to_list()
                if len(matching_codes) > 1:
                    model_code = matching_codes[0]
                    print('multiple codes matched one model, whats up with that')
                elif len(matching_codes) == 1:
                    model_code = matching_codes[0]
            # else:
                # print(f'no model matched {model}')
                
        return model_code

    def check_client_code(self, code):
        primary = self.database['clients'].filter(pl.col('client_code') == code)

        # columns_to_check = ['scrape', 'alt 1', 'alt 2']
        scraping = self.database['clients'].filter(
            (pl.col('scrape') == code ) | (pl.col('alt 1') == code ) | (pl.col('alt 2') == code)
        )
        
        return primary, scraping
        
    def check_model_code(self, code):
        found = self.database['methods'].filter(pl.col('method_code') == code)
        return found

    def add_new_client(self, client, code, overwrite = False):
        written = self.write_entry('clients', 
                         data = {'scrape': code, 'alt 1': None, 'alt 2': None, 'client_code': code, 'client': client, 'Search 1': client}, 
                         key = {'client_code': code, 'client': client}, overwrite=overwrite)
        
        return written
    
    def add_new_method(self, method, code, overwrite = False):
        written = self.write_entry('methods',
                                   data = {'method_code': code, 'method': method, 'Search 1': method},
                                   key =  {'method_code': code, 'method': method} )
        
        return written





        