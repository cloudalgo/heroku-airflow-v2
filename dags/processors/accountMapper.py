import pandas as pd

class AccountMapper(DataProcessor):
    def __init__(self, config_file):
        super().__init__(config_file)

    def _perform_processing(self, data, field_mapping):
        # Perform the data processing operations specific to account mapping
        for field, mapping in field_mapping.items():
            if "columns" in mapping:
                columns = mapping["columns"]
                data[mapping["column_name"]] = data[columns].apply(lambda x: ''.join(x), axis=1)
                if "data_manipulation" in mapping:
                    for manipulation_func in mapping["data_manipulation"]:
                        data[mapping["column_name"]] = self._apply_data_manipulation(data[mapping["column_name"]], manipulation_func)
            else:
                # Single column mapping
                if "data_manipulation" in mapping:
                    for manipulation_func in mapping["data_manipulation"]:
                        data[mapping["column_name"]] = self._apply_data_manipulation(data[mapping["column_name"]], manipulation_func)

        return data

    def _apply_data_manipulation(self, data, manipulation_func):
        if manipulation_func == 'uppercase':
            data = data.str.upper()
        elif manipulation_func == 'abs':
            data = data.abs()
        elif manipulation_func == 'round':
            data = data.round()
        elif manipulation_func == 'format_date':
            data = data.apply(lambda x: datetime.strftime(x, '%Y-%m-%d'))
        elif manipulation_func.startswith('change_type:'):
            new_type = manipulation_func.split(':')[1]
            try:
                data = data.astype(new_type)
            except ValueError as e:
                print(f"Error converting column to {new_type}: {e}")
        else:
            print(f"Unsupported data manipulation function: {manipulation_func}")

        return data
    
    def _validate_data(self, data, field_mapping):
        valid_rows = []

        for _, row in data.iterrows():
            is_valid = True

            for field, mapping in field_mapping.items():
                # Check if field is required
                if 'validation' in mapping and mapping['validation'].get('required', False):
                    if pd.isnull(row[field]):
                        print(f"Missing required field: {field}")
                        is_valid = False
                        continue

                # Check validation rules based on data type
                if 'data_manipulation' in mapping:
                    for manipulation_func in mapping['data_manipulation']:
                        if manipulation_func.startswith('change_type:'):
                            data_type = manipulation_func.split(':')[1]

                            # Check numeric field rules
                            if data_type in ['int', 'float']:
                                value = row[field]
                                min_value = mapping['validation'].get('min_value')
                                max_value = mapping['validation'].get('max_value')

                                if min_value is not None and value < min_value:
                                    print(f"Value of '{field}' is below the minimum allowed value.")
                                    is_valid = False

                                if max_value is not None and value > max_value:
                                    print(f"Value of '{field}' is above the maximum allowed value.")
                                    is_valid = False

                            # Check string field rules
                            elif data_type == 'str':
                                value = str(row[field])
                                min_length = mapping['validation'].get('min_length')
                                max_length = mapping['validation'].get('max_length')

                                if min_length is not None and len(value) < min_length:
                                    print(f"Length of '{field}' is below the minimum allowed length.")
                                    is_valid = False

                                if max_length is not None and len(value) > max_length:
                                    print(f"Length of '{field}' is above the maximum allowed length.")
                                    is_valid = False

            if is_valid:
                valid_rows.append(row)

        return pd.DataFrame(valid_rows)
