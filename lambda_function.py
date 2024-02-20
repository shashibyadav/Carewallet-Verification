import boto3
import json
import sys
import io
import numpy as np

from insurance_ocr_nlp import insurance_ocr_step
from update_database import update_dynamodb_item, update_dynamodb_attributesy
from PIL import Image

def fraud_step():
    pass

def rename_file(root_name, old_ext, new_ext):
    return root_name.replace(old_ext, new_ext)

def crop_step(s3_client, bucket_name, folder_name, image, image_name, bbox):
    x1, y1, x2, y2 = bbox
    image = image.crop((x1, y1, x2, y2))
    #bytesarray = bytes(Image.fromarray(np.array(image).reshape((image.height, image.width, 4))).tobytes())
    
    buffered = io.BytesIO()
    image.save(buffered, format="PNG")  # Change format if necessary
    
    # I dont feel like messing this up
    new_fname = rename_file(image_name, '.jpg', '-cropped.jpg')
    new_fname = rename_file(new_fname, '.JPG', '-cropped.jpg')
    new_fname = rename_file(new_fname, '.png', '-cropped.png')
    new_fname = rename_file(new_fname, '.PNG', '-cropped.png')
    
    s3_client.put_object(Body=buffered.getvalue(), 
                         Bucket=bucket_name, 
                         Key=new_fname
                        )      
                        
    return new_fname


def get_face_bb(id_filename, bucket_name, folder_name, region, s3_client, s3_connection, rekognition_client):
    def get_bounding_box(response_bbox, img_width, img_height):
        bbox = None
        left = img_width * response_bbox['Left']    
        top = img_height * response_bbox['Top']    
        width = img_width * response_bbox['Width']    
        height = img_height * response_bbox['Height']              
            
        #dimenions of famous face inside the bounding boxes    
        x1=left    
        y1=top    
        x2=left+width    
        y2=top+height
        
        bbox = [x1, y1, x2, y2]

        return bbox
    
    x1, y1, x2, y2 = None, None, None, None
    
    
    response = rekognition_client.detect_faces(Image={'S3Object': {'Bucket':bucket_name,
                                                                  'Name': id_filename}},
                                               Attributes=['DEFAULT'])
    s3_object = s3_connection.Object(bucket_name, id_filename)
    s3_response = s3_object.get()
    stream = io.BytesIO(s3_response['Body'].read())

    image = Image.open(stream)
    img_width, img_height = image.size
    
    try:
        [x1, y1, x2, y2] = get_bounding_box(response['FaceDetails'][0]['BoundingBox'], img_width, img_height)
        return image, [x1, y1, x2, y2]
    except:
        return None, None
            

def compare_faces(bucket_name, rekognition_client, source_key, target_key, thresh=75):
    response = rekognition_client.compare_faces(
        SimilarityThreshold=thresh,
        SourceImage={'S3Object': {'Bucket': bucket_name, 'Name': source_key}},
        TargetImage={'S3Object': {'Bucket': bucket_name, 'Name': target_key}}
    )

    similarity = None
    for faceMatch in response['FaceMatches']:
        similarity = faceMatch['Similarity']

    return similarity
    
def ocr_step(s3_client, textract_client, id_filename, bucket_name, folder_name):
    extracted_text = textract_client.analyze_id(
        DocumentPages=[{'S3Object': {'Bucket': bucket_name, 'Name': id_filename}}])
    
    json_key = rename_file(id_filename, '.jpg', '.json')
    json_key = rename_file(json_key, '.JPG', '.json')
    json_key = rename_file(json_key, '.png', '.json')
    json_key = rename_file(json_key, '.PNG', '.json')

    # Save extracted text as JSON in S3
    s3_client.put_object(
        Bucket=bucket_name,
        Key=json_key,
        Body=json.dumps(extracted_text),
        ContentType='application/json'
    )
    return True
    

def lambda_handler(event, context): 
    log_message = ''
    # Initialize AWS clients
    s3_client = boto3.client('s3')
    bucket_name = 'carewallet'
    folder_name = 'temp/'

    SIM_THRESH = 97
    s3_connection = boto3.resource('s3')
    #s3_client = boto3.client('s3')
    textract_client = boto3.client('textract')
    rekognition_client = boto3.client('rekognition')
    
    # Specify your S3 bucket name and folder name
    bucket_name = 'carewallet'
    folder_name = 'temp/'
    region_name = 'us-east-1'
    table_name = 'temp-session-data'
    
    # scan_image_name = f'{folder_name}usr1738-selfie.png'
    # id_image_name = f'{folder_name}usr1738-id.png'
    # insurance_card_image_name = f'{folder_name}usr1738-insurance.png'

    scan_image_name = event['userPhoto']
    id_image_name = event['govIDFront']
    insurance_card_image_name = event['insuranceFront']
    item_id = event['id']
  
    
    # handle a running boolean for verification status
    verified = False
    
    # handles fraud detection step
    fraud_step()

    # detect faces in crop
    image, bbox = get_face_bb(id_image_name, bucket_name, folder_name, region_name, \
                                s3_client, s3_connection, rekognition_client)
    if not image:
        update_dynamodb_attributes(
            item_id,
            [('status', 'VERIFICATION_FAILED')], 
            primary_key_value='id',
            table_name=table_name, 
            table=None
        )
        # update_dynamodb_item(item_id, 'status', 'VERIFICATION_FAILED', table_name, table=None)
        return {
            'statusCode': -1,
            'body': json.dumps(f'Rejected. no face found.')
        }
    

    cropped_id_name = crop_step(s3_client, bucket_name, folder_name, image, id_image_name, bbox)
    if cropped_id_name:
        print(f'Success! Found a face and cropped it to {cropped_id_name}.')
        verified = True
    else:
        print('Did not provide a high-quality ID image.')

    sim = compare_faces(bucket_name, rekognition_client, scan_image_name, cropped_id_name, thresh=90)
    if not sim:
        sim = 0
    verified = (sim > SIM_THRESH) and verified
    if verified:
        print(f'Similarity of {sim}%! You pass.')
    else:
        print(f'You are not who you say you are; only {sim}% similar.')
        
    # ocr_outnames = ocr_step(s3_client, textract_client, id_image_name, bucket_name, folder_name)
    
    #todo: make connection with dynmodb in this lambda
    #the fields we want: groupnumber, insurance plan type, relationship to policyholder (self), memberid, member dob (resolve with gov ID), effectivedate

    insurance_response = insurance_ocr_step(insurance_card_image_name, bucket_name, 
    s3_client, textract_client)
    
    if verified:
        insurance_response['status'] = 'VERIFICATION_SUCCESS'
        update_dynamodb_attributes(
            item_id,
            insurance_response.items(), 
            primary_key_value='id',
            table_name=table_name, 
            table=None
        )
        
        return {
            'statusCode': 200,
            'body': json.dumps(f'Congrats! You have verified your account.')
        }
        
    else:
        update_dynamodb_attributes(
            item_id,
            [('status', 'VERIFICATION_FAILED')], 
            primary_key_value='id',
            table_name=table_name, 
            table=None
        )
        # update_dynamodb_item(item_id, 'status', 'VERIFICATION_FAILED', table_name, table=None)
        return {
            'statusCode': -1,
            'body': json.dumps(f'Rejected due to low ({sim}%) similarity.')
        }
    
