
def meld_columns(rows, columns):
	result = []
	for row in rows:
		result.append(dict(zip(columns, row)))
	return result

def group_by_column(data, column):
	result = {}
	for item in data:
		if(item[column] not in result):
			result[item[column]] = []
		result[item[column]].append(item)

	return result