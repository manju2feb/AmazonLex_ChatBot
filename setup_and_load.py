import boto3

def lambda_handler(event, context):
	
	#calling setup method create table schema
	setup()
	# Get the service resource.
	dynamodb = boto3.resource('dynamodb')
	table = dynamodb.Table('BookMyShow')
	
	# Load table 'BookMyShow' with dummy data
	status = load_dummy_data(table, data)
	
	if status:
		return "Table created successfully and loaded with dummy data"
	else:
		return "Some Error occured!"

 
def setup():
	# Get the service resource.
	dynamodb = boto3.resource('dynamodb')
	
	# Create the DynamoDB table
	table = dynamodb.create_table(
		TableName= 'BookMyShow',
		KeySchema=[
			{
				'AttributeName': 'PK',
				'KeyType': 'HASH'
			},
			{
				'AttributeName': 'SK',
				'KeyType': 'RANGE'
			}
		],
		AttributeDefinitions=[
			{
				'AttributeName': 'PK',
				'AttributeType': 'S'
			},
			{
				'AttributeName': 'SK',
				'AttributeType': 'S'
			},
			{
				'AttributeName': 'GSI1',
				'AttributeType': 'S'
			}
		],
		GlobalSecondaryIndexes=[
			{
				'IndexName': 'GSI1-SK-index',
				'KeySchema': [
					{
						'AttributeName': 'GSI1',
						'KeyType': 'HASH'
					},
					{
						'AttributeName': 'SK',
						'KeyType': 'RANGE'
					}
				],
				'Projection': {
					'ProjectionType': 'ALL'
				},
				'ProvisionedThroughput': {
					'ReadCapacityUnits': 2,
					'WriteCapacityUnits': 2
				}
            },
            {
				'IndexName': 'SK-PK-index',
				'KeySchema': [
					{
						'AttributeName': 'SK',
						'KeyType': 'HASH'
					},
					{
						'AttributeName': 'PK',
						'KeyType': 'RANGE'
					}
				],
				'Projection': {
					'ProjectionType': 'ALL'
				},
				'ProvisionedThroughput': {
					'ReadCapacityUnits': 2,
					'WriteCapacityUnits': 2
				}
            }
        ],
        ProvisionedThroughput={
        	'ReadCapacityUnits': 5,
        	'WriteCapacityUnits': 5
        })
    #Wait until the table exists.
	table.meta.client.get_waiter('table_exists').wait(TableName='BookMyShow')
    
def write_items_to_db(table, items):
    try:
        with table.batch_writer() as batch:
            for i in items:
                batch.put_item(Item=i)
        return True
    except:
        return False

def load_dummy_data(table, data):
	status = False
	try:
		for key in data.keys():
			status = write_items_to_db(table, data[key])
		return status
	except:
		return status
	
data = {
	"genres":[
		{"PK":"gen101","SK":"genre","GenreName":"action"},
		{"PK":"gen102","SK":"genre","GenreName":"adventure"},
		{"PK":"gen103","SK":"genre","GenreName":"animation"},
		{"PK":"gen104","SK":"genre","GenreName":"comedy"},
		{"PK":"gen105","SK":"genre","GenreName":"crime"},
		{"PK":"gen106","SK":"genre","GenreName":"drama"},
		{"PK":"gen107","SK":"genre","GenreName":"fanasy"},
		{"PK":"gen108","SK":"genre","GenreName":"horror"},
		{"PK":"gen109","SK":"genre","GenreName":"mystery"},
		{"PK":"gen110","SK":"genre","GenreName":"romance"},
		{"PK":"gen111","SK":"genre","GenreName":"sci-fi"},
		{"PK":"gen112","SK":"genre","GenreName":"thriller"}
	],
	"movies":[
		{"PK":"mov101","SK":"movie","GSI1":"The Dark Knight"},
		{"PK":"mov102","SK":"movie","GSI1":"The Avengers"},
		{"PK":"mov103","SK":"movie","GSI1":"Jurassic Park"},
		{"PK":"mov104","SK":"movie","GSI1":"Despicable Me"},
		{"PK":"mov105","SK":"movie","GSI1":"Kung Fu Panda"},
		{"PK":"mov106","SK":"movie","GSI1":"Home Alone"},
		{"PK":"mov107","SK":"movie","GSI1":"Kick Ass"},
		{"PK":"mov108","SK":"movie","GSI1":"Jocker"},
		{"PK":"mov109","SK":"movie","GSI1":"Thor: Ragnarok"},
		{"PK":"mov110","SK":"movie","GSI1":"The Conjuring"},
		{"PK":"mov111","SK":"movie","GSI1":"Annabelle: Creation"},
		{"PK":"mov112","SK":"movie","GSI1":"Prisoners"},
		{"PK":"mov113","SK":"movie","GSI1":"Murder Mystery"},
		{"PK":"mov114","SK":"movie","GSI1":"Titanic"},
		{"PK":"mov115","SK":"movie","GSI1":"The Notebook"},
		{"PK":"mov116","SK":"movie","GSI1":"The Departed"}
	],
	"theaters":[
		{"PK":"theater101","SK":"theater","GSI1":"Fox Theater","Location":"Atlanta"},
		{"PK":"theater102","SK":"theater","GSI1":"Kentucky Theater","Location":"Lexington"},
		{"PK":"theater103","SK":"theater","GSI1":"Oriental Theater","Location":"Milwaukee"},
		{"PK":"theater104","SK":"theater","GSI1":"Tampa Theater","Location":"Tampa"},
		{"PK":"theater105","SK":"theater","GSI1":"Film Forum","Location":"New York"},
		{"PK":"theater106","SK":"theater","GSI1":"Nitehawk Cinema","Location":"New York"},
		{"PK":"theater107","SK":"theater","GSI1":"Congress Theater","Location":"Chicago"},
		{"PK":"theater108","SK":"theater","GSI1":"Michigan Theater","Location":"Ann Arbor"},
		{"PK":"theater109","SK":"theater","GSI1":"Plaza Cinema","Location":"Ottawa"},
		{"PK":"theater110","SK":"theater","GSI1":"The Paris Theater","Location":"New York"}
	],
	"moviegenres":[
		{"PK":"mov101","SK":"moviegenre101","GSI1":"gen101"},
		{"PK":"mov101","SK":"moviegenre105","GSI1":"gen105"},
		{"PK":"mov101","SK":"moviegenre106","GSI1":"gen106"},
		{"PK":"mov102","SK":"moviegenre101","GSI1":"gen101"},
		{"PK":"mov102","SK":"moviegenre102","GSI1":"gen102"},
		{"PK":"mov112","SK":"moviegenre112","GSI1":"gen112"},
		{"PK":"mov102","SK":"moviegenre107","GSI1":"gen107"},
		{"PK":"mov102","SK":"moviegenre111","GSI1":"gen111"},
		{"PK":"mov103","SK":"moviegenre102","GSI1":"gen102"},
		{"PK":"mov103","SK":"moviegenre111","GSI1":"gen111"},
		{"PK":"mov104","SK":"moviegenre103","GSI1":"gen103"},
		{"PK":"mov105","SK":"moviegenre103","GSI1":"gen103"},
		{"PK":"mov106","SK":"moviegenre104","GSI1":"gen104"},
		{"PK":"mov107","SK":"moviegenre104","GSI1":"gen104"},
		{"PK":"mov108","SK":"moviegenre105","GSI1":"gen105"},
		{"PK":"mov108","SK":"moviegenre106","GSI1":"gen106"},
		{"PK":"mov109","SK":"moviegenre107","GSI1":"gen107"},
		{"PK":"mov110","SK":"moviegenre108","GSI1":"gen108"},
		{"PK":"mov111","SK":"moviegenre108","GSI1":"gen108"},
		{"PK":"mov112","SK":"moviegenre109","GSI1":"gen109"},
		{"PK":"mov113","SK":"moviegenre109","GSI1":"gen109"},
		{"PK":"mov114","SK":"moviegenre110","GSI1":"gen110"},
		{"PK":"mov115","SK":"moviegenre110","GSI1":"gen110"},
		{"PK":"mov116","SK":"moviegenre112","GSI1":"gen112"}
	],
	"movietheaters":[
		{"PK":"mov101","SK":"theater101","show_from_date":"2020-07-19","show_to_date":"2020-08-20"},
		{"PK":"mov101","SK":"theater102","show_from_date":"2020-07-19","show_to_date":"2020-08-20"},
		{"PK":"mov101","SK":"theater106","show_from_date":"2020-07-19","show_to_date":"2020-08-20"},
		{"PK":"mov102","SK":"theater102","show_from_date":"2020-07-19","show_to_date":"2020-08-20"},
		{"PK":"mov102","SK":"theater108","show_from_date":"2020-07-19","show_to_date":"2020-08-20"},
		{"PK":"mov102","SK":"theater110","show_from_date":"2020-07-19","show_to_date":"2020-08-20"},
		{"PK":"mov103","SK":"theater103","show_from_date":"2020-07-19","show_to_date":"2020-08-20"},
		{"PK":"mov105","SK":"theater105","show_from_date":"2020-07-19","show_to_date":"2020-08-20"},
		{"PK":"mov103","SK":"theater105","show_from_date":"2020-07-19","show_to_date":"2020-08-20"},
		{"PK":"mov103","SK":"theater106","show_from_date":"2020-07-19","show_to_date":"2020-08-20"},
		{"PK":"mov104","SK":"theater103","show_from_date":"2020-07-19","show_to_date":"2020-08-20"},
		{"PK":"mov105","SK":"theater108","show_from_date":"2020-07-19","show_to_date":"2020-08-20"},
		{"PK":"mov106","SK":"theater105","show_from_date":"2020-07-19","show_to_date":"2020-08-20"},
		{"PK":"mov104","SK":"theater108","show_from_date":"2020-07-19","show_to_date":"2020-08-20"},
		{"PK":"mov105","SK":"theater109","show_from_date":"2020-07-19","show_to_date":"2020-08-20"},
		{"PK":"mov106","SK":"theater102","show_from_date":"2020-07-19","show_to_date":"2020-08-20"},
		{"PK":"mov104","SK":"theater110","show_from_date":"2020-07-19","show_to_date":"2020-08-20"},
		{"PK":"mov106","SK":"theater101","show_from_date":"2020-07-19","show_to_date":"2020-08-20"},
		{"PK":"mov107","SK":"theater105","show_from_date":"2020-07-19","show_to_date":"2020-08-20"},
		{"PK":"mov107","SK":"theater106","show_from_date":"2020-07-19","show_to_date":"2020-08-20"},
		{"PK":"mov107","SK":"theater110","show_from_date":"2020-07-19","show_to_date":"2020-08-20"},
		{"PK":"mov108","SK":"theater105","show_from_date":"2020-07-19","show_to_date":"2020-08-20"},
		{"PK":"mov113","SK":"theater107","show_from_date":"2020-07-19","show_to_date":"2020-08-20"},
		{"PK":"mov108","SK":"theater107","show_from_date":"2020-07-19","show_to_date":"2020-08-20"},
		{"PK":"mov108","SK":"theater108","show_from_date":"2020-07-19","show_to_date":"2020-08-20"},
		{"PK":"mov109","SK":"theater104","show_from_date":"2020-07-19","show_to_date":"2020-08-20"},
		{"PK":"mov109","SK":"theater105","show_from_date":"2020-07-19","show_to_date":"2020-08-20"},
		{"PK":"mov109","SK":"theater108","show_from_date":"2020-07-19","show_to_date":"2020-08-20"},
		{"PK":"mov110","SK":"theater101","show_from_date":"2020-07-19","show_to_date":"2020-08-20"},
		{"PK":"mov110","SK":"theater103","show_from_date":"2020-07-19","show_to_date":"2020-08-20"},
		{"PK":"mov111","SK":"theater104","show_from_date":"2020-07-19","show_to_date":"2020-08-20"},
		{"PK":"mov112","SK":"theater105","show_from_date":"2020-07-19","show_to_date":"2020-08-20"},
		{"PK":"mov110","SK":"theater104","show_from_date":"2020-07-19","show_to_date":"2020-08-20"},
		{"PK":"mov111","SK":"theater103","show_from_date":"2020-07-19","show_to_date":"2020-08-20"},
		{"PK":"mov112","SK":"theater108","show_from_date":"2020-07-19","show_to_date":"2020-08-20"},
		{"PK":"mov111","SK":"theater109","show_from_date":"2020-07-19","show_to_date":"2020-08-20"},
		{"PK":"mov112","SK":"theater104","show_from_date":"2020-07-19","show_to_date":"2020-08-20"},
		{"PK":"mov113","SK":"theater101","show_from_date":"2020-07-19","show_to_date":"2020-08-20"},
		{"PK":"mov113","SK":"theater110","show_from_date":"2020-07-19","show_to_date":"2020-08-20"},
		{"PK":"mov114","SK":"theater101","show_from_date":"2020-07-19","show_to_date":"2020-08-20"},
		{"PK":"mov114","SK":"theater107","show_from_date":"2020-07-19","show_to_date":"2020-08-20"},
		{"PK":"mov116","SK":"theater109","show_from_date":"2020-07-19","show_to_date":"2020-08-20"},
		{"PK":"mov114","SK":"theater110","show_from_date":"2020-07-19","show_to_date":"2020-08-20"},
		{"PK":"mov115","SK":"theater101","show_from_date":"2020-07-19","show_to_date":"2020-08-20"},
		{"PK":"mov115","SK":"theater105","show_from_date":"2020-07-19","show_to_date":"2020-08-20"},
		{"PK":"mov115","SK":"theater107","show_from_date":"2020-07-19","show_to_date":"2020-08-20"},
		{"PK":"mov116","SK":"theater104","show_from_date":"2020-07-19","show_to_date":"2020-08-20"},
		{"PK":"mov116","SK":"theater106","show_from_date":"2020-07-19","show_to_date":"2020-08-20"}
	]
}
