import boto3
import json
import sys
import io

from insurance_ocr_nlp import insurance_ocr_step
from update_database import update_dynamodb_item, update_dynamodb_items_driver

import numpy as np

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
    
    new_fname = rename_file(image_name, 'id.png', 'id-cropped.png')
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
    
    [x1, y1, x2, y2] = get_bounding_box(response['FaceDetails'][0]['BoundingBox'], img_width, img_height)
            
    return image, [x1, y1, x2, y2]

def compare_faces(bucket_name, rekognition_client, source_key, target_key, thresh=90):
    response = rekognition_client.compare_faces(
        SimilarityThreshold=thresh,
        SourceImage={'S3Object': {'Bucket': bucket_name, 'Name': source_key}},
        TargetImage={'S3Object': {'Bucket': bucket_name, 'Name': target_key}}
    )

    for faceMatch in response['FaceMatches']:
        similarity = faceMatch['Similarity']

    return similarity
    
def ocr_step(s3_client, textract_client, id_filename, bucket_name, folder_name):
    extracted_text = textract_client.analyze_id(
        DocumentPages=[{'S3Object': {'Bucket': bucket_name, 'Name': id_filename}}])
    json_key = rename_file(id_filename, '.png', '.json')

    # Save extracted text as JSON in S3
    s3_client.put_object(
        Bucket=bucket_name,
        Key=json_key,
        Body=json.dumps(extracted_text),
        ContentType='application/json'
    )
    return True
    

def lambda_handler(event, context): 
    # Initialize AWS clients
    # s3_client = boto3.client('s3')
    # bucket_name = 'carewallet-patients'
    # folder_name = 'persistent/'
    # s3_client.put_object(Body=json.dumps(event), 
    #                      Bucket=bucket_name, 
    #                      Key=f'{folder_name}TEST_TRIGGER.json'
    #                     )      
    # return
    
    s3_connection = boto3.resource('s3')
    textract_client = boto3.client('textract')
    rekognition_client = boto3.client('rekognition')

    # Specify your S3 bucket name and folder name
    
    bucket_name = 'carewallet-patients'
    folder_name = 'persistent/'
    region_name = 'us-east-1'
    
    scan_image_name = f'{folder_name}usr1738-selfie.png'
    id_image_name = f'{folder_name}usr1738-id.png'
    insurance_card_image_name = f'{folder_name}usr1738-insurance.png'
    
    item_id = 'NQc-H2nE0-StBRbGjYnRPToWwoOncUvh'
    table_name = 'temp-session-data'
    
    
    # handles fraud detection step
    fraud_step()
    
    # detect faces in crop
    image, bbox = get_face_bb(id_image_name, bucket_name, folder_name, region_name, \
                                s3_client, s3_connection, rekognition_client)

    # handles facial matching step
    cropped_id_name = crop_step(s3_client, bucket_name, folder_name, image, id_image_name, bbox)
    if cropped_id_name:
        print(f'Success! Found a face and cropped it to {cropped_id_name}.')
    else:
        raise Exception('Did not provide a high-quality ID image.')
    
    sim = compare_faces(bucket_name, rekognition_client, scan_image_name, cropped_id_name, thresh=90)
    verified = sim > 98
    if verified:
        print(f'Similarity of {sim}%! You pass.')
    else:
        print('You are not who you say you are!')

    ocr_outnames = ocr_step(s3_client, textract_client, id_image_name, bucket_name, folder_name)
    
    #todo: make connection with dynmodb in this lambda
    #the fields we want: groupnumber, insurance plan type, relationship to policyholder (self), memberid, member dob (resolve with gov ID), effectivedate

    insurance_response = insurance_ocr_step(insurance_card_image_name, bucket_name, 
    s3_client, textract_client)

    if verified:
        insurance_response['status'] = 'VERIFIED'
        for attribute, new_value in insurance_response.items():
            # print('attribute, value', attribute, new_value)
            update_dynamodb_item(item_id, attribute, new_value, table_name, table=None)
        
        return {
            'statusCode': 200,
            'body': json.dumps(f'Text extraction completed successfully; saved json files at: {ocr_outnames}.')
        }
        
    else:
        update_dynamodb_item(item_id, 'status', 'NOT_VERIFIED', table_name, table=None)
    
