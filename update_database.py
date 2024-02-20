import boto3
import json

def update_dynamodb_items_driver(dynamodb, table_name, attribute_names, new_values):
    # given a list of new attribute values
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(table_name)
    
    for att_name, value in zip(attribute_names, new_values):
        update_dynamodb_item(table, item_id, attribute_name, new_value)

def update_dynamodb_item(item_id, attribute_name, new_value, table_name=None, table=None):
    if not table:
        if table_name:
            dynamodb = boto3.resource('dynamodb')
            table = dynamodb.Table(table_name)
        else:
            raise ValueError('No table id provided.')
    try:
        if attribute_name == 'status':
            response = table.update_item(
                Key={
                    'id': item_id
                },
                UpdateExpression=f'SET #status = :new_value',  
                ExpressionAttributeNames={
                    '#status': 'status'
                },
                ExpressionAttributeValues={
                    ':new_value': new_value
                }
            )
        else:
            response = table.update_item(
                Key={
                    'id': item_id
                },
                UpdateExpression=f'SET {attribute_name} = :new_value',  
                ExpressionAttributeValues={
                    ':new_value': new_value
                }
            )

        updated_item = response.get('Attributes', {})
        #print(f"Item with ID {item_id} updated successfully. New value: {updated_item}")
    except Exception as e:
        print(f"Error updating item: {e}")

    return response
    
def update_dynamodb_attributes(item_id, attribute_updates, primary_key_value='id', table_name=None, table=None):
    if not table:
        if table_name:
            dynamodb = boto3.resource('dynamodb')
            table = dynamodb.Table(table_name)
        else:
            raise ValueError('No table id provided.')

    update_expression = "SET "
    expression_attribute_values = {f":val_{idx}": value for idx, (_, value) in enumerate(attribute_updates)}

    for idx, (attribute_name, _) in enumerate(attribute_updates):
        if attribute_name == 'status':
            attribute_name = '#status'
        update_expression += f"{attribute_name} = :val_{idx}, "
        
    update_expression = update_expression.rstrip(", ")

    update_item_request = {
        'TableName': table_name,
        'Key': {
            primary_key_value : item_id
        },
        'UpdateExpression': update_expression,
        'ExpressionAttributeNames' : {
            '#status' : 'status'
        },
        'ExpressionAttributeValues': expression_attribute_values
    }
    
    s3_client = boto3.client('s3')
    s3_client.put_object(
        Bucket='carewallet-patients',
        Key='persistent/final_update_query.json',
        Body=json.dumps(update_item_request),
        ContentType='application/json'
    )

    table.update_item(**update_item_request)
