import traceback
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
from apify_client import ApifyClient
import requests
import os
import json

import uuid
from dotenv import load_dotenv
from pathlib import Path

# Force load .env from the current app directory
env_path = Path(__file__).parent / ".env"
load_dotenv(dotenv_path=env_path)


@dataclass
class ProcessedError:
    type: str
    description: str
    place_id: Optional[str] = None
    review_id: Optional[str] = None
    business_info: Optional[Dict[str, Any]] = None

@dataclass
class ProcessedBusinessInfo:
    place_id: Optional[str] = None
    title: Optional[str] = None
    category_name: Optional[str] = None
    categories: Optional[str] = None
    address: Optional[str] = None
    neighborhood: Optional[str] = None
    street: Optional[str] = None
    city: Optional[str] = None
    postal_code: Optional[str] = None
    state: Optional[str] = None
    country_code: Optional[str] = None
    location: Optional[Any] = None
    total_score: Optional[float] = None
    reviews_count: Optional[int] = None
    price: Optional[int] = None
    permanently_closed: Optional[bool] = None
    temporarily_closed: Optional[bool] = None
    image_url: Optional[str] = None
    url: Optional[str] = None
    cid: Optional[str] = None
    fid: Optional[str] = None

CORS_HEADERS = {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Headers": "authorization, x-client-info, apikey, content-type",
    "Access-Control-Allow-Methods": "GET, POST, PUT, DELETE, OPTIONS",
    "Content-Type": "application/json"
}

def extract_place_id(url: str) -> Optional[str]:
    import re
    match = re.search(r'\?cid=(\d+)', url)
    return match.group(1) if match else None

def clean_review_data(item):
    """Clean review data to ensure it's JSON serializable"""
    cleaned_item = {}
    for key, value in item.items():
        if value is None:
            cleaned_item[key] = None
        elif isinstance(value, (str, int, float, bool)):
            cleaned_item[key] = value
        elif isinstance(value, (list, dict)):
            try:
                # Test if it's serializable
                json.dumps(value)
                cleaned_item[key] = value
            except (TypeError, ValueError):
                cleaned_item[key] = str(value)
        else:
            # Convert complex objects to string
            cleaned_item[key] = str(value)
    return cleaned_item

def safe_json_dumps(obj):
    """Safely serialize objects to JSON, handling complex types"""
    try:
        return json.dumps(obj, default=str)
    except Exception as e:
        return json.dumps({"error": "Serialization failed", "details": str(e)})

def lambda_handler(event, context):
    if event.get('httpMethod') == 'OPTIONS':
        return {
            'statusCode': 200,
            'headers': CORS_HEADERS,
            'body': json.dumps({'message': 'OK'})
        }

    # Check if this is a status check request
    if event.get('httpMethod') == 'GET' and '/scraping-status/' in event.get('path', ''):
        return handle_status_check(event)

    # Use a real UUID for scraping_attempt_id unless provided
    scraping_attempt_id = str(uuid.uuid4())

    try:
        # Parse request body
        try:
            body = json.loads(event.get('body', '{}'))
        except json.JSONDecodeError as e:
            return {
                'statusCode': 400,
                'headers': CORS_HEADERS,
                'body': json.dumps({'error': 'Invalid JSON in request body', 'details': str(e)})
            }

        place_ids = body.get('placeIds', [])
        max_reviews = body.get('maxReviews', 5)
        reviews_start_date = body.get('reviewsStartDate')
        scraping_metadata_id = body.get('scraping_metadata_id')
        return_immediately = body.get('returnImmediately', True)  # New parameter
        user_profile_id = body.get('user_profile_id')

        if not place_ids:
            return {
                'statusCode': 400,
                'headers': CORS_HEADERS,
                'body': json.dumps({'error': 'Place IDs are required'})
            }

        if not user_profile_id:
            return {
                'statusCode': 400,
                'headers': CORS_HEADERS,
                'body': json.dumps({'error': 'user_profile_id is required'})
            }

        if not scraping_metadata_id:
            # Use a real UUID if not provided
            scraping_attempt_id = str(uuid.uuid4())
        else:
            scraping_attempt_id = scraping_metadata_id
        business_place_id = place_ids[0]

        # Database configuration - Fetch from environment variables
        supabase_url = os.getenv("SUPABASE_URL")
        supabase_key = os.getenv("SUPABASE_KEY")

        if not supabase_url or not supabase_key:
            return {
                'statusCode': 500,
                'headers': CORS_HEADERS,
                'body': json.dumps({'error': 'Supabase URL or Key not configured in environment variables'})
            }

        supabase_headers = {
            'apikey': supabase_key,
            'Authorization': f'Bearer {supabase_key}',
            'Content-Type': 'application/json',
            'Prefer': 'return=representation'
        }

        # If return_immediately is True, invoke another Lambda asynchronously and return immediately
        if return_immediately:
            try:
                import boto3
                lambda_client = boto3.client('lambda')

                # Invoke the same Lambda function asynchronously with a flag to do the actual work
                async_payload = {
                    'isAsyncExecution': True,
                    'scrapingAttemptId': scraping_attempt_id,
                    'placeIds': place_ids,
                    'maxReviews': max_reviews,
                    'reviewsStartDate': reviews_start_date,
                    'businessPlaceId': business_place_id
                }

                # Get current function name from context
                function_name = context.function_name

                lambda_client.invoke(
                    FunctionName=function_name,
                    InvocationType='Event',  # Asynchronous invocation
                    Payload=json.dumps(async_payload)
                )

                return {
                    'statusCode': 202,
                    'headers': CORS_HEADERS,
                    'body': json.dumps({
                        'message': 'Scraping initiated successfully',
                        'scrapingAttemptId': scraping_attempt_id,
                        'status': 'in_progress',
                        'estimatedDuration': '10-15 minutes'
                    })
                }

            except Exception as async_error:
                print(f"Failed to invoke async Lambda: {str(async_error)}")
                # Fall back to synchronous execution
                return_immediately = False

        # Handle async execution (when invoked by the async trigger above)
        if event.get('isAsyncExecution'):
            return handle_async_scraping(event, supabase_url, supabase_headers)

        # Synchronous execution (original behavior)
        return handle_synchronous_scraping(
            scraping_attempt_id, place_ids, max_reviews, reviews_start_date,
            business_place_id, supabase_url, supabase_headers, user_profile_id
        )

    except Exception as error:
        error_msg = str(error)
        error_type = type(error).__name__
        full_traceback = traceback.format_exc()

        # If we have a scraping attempt ID, update the metadata
        if scraping_attempt_id:
            try:
                # Database configuration (using environment variables here too)
                supabase_url = os.getenv("SUPABASE_URL")
                supabase_key = os.getenv("SUPABASE_KEY")

                if supabase_url and supabase_key: # Only attempt update if keys are available
                    supabase_headers = {
                        'apikey': supabase_key,
                        'Authorization': f'Bearer {supabase_key}',
                        'Content-Type': 'application/json',
                        'Prefer': 'return=representation'
                    }
                    requests.patch(
                        f'{supabase_url}/rest/v1/scraping_metadata?id=eq.{scraping_attempt_id}',
                        headers=supabase_headers,
                        data=json.dumps({
                            'scraping_status': 'failed',
                            'error_message': f'{error_type}: {error_msg}'
                        }),
                        timeout=30
                    ).raise_for_status()
                else:
                    print("Supabase environment variables not set, cannot update metadata on error.")
            except Exception as update_error:
                print(f"Failed to update metadata on error: {str(update_error)}")

        print(f"Lambda function error: {full_traceback}")

        return {
            'statusCode': 500,
            'headers': CORS_HEADERS,
            'body': json.dumps({
                'error': 'Internal server error',
                'details': f'{error_type}: {error_msg}',
            })
        }

def handle_async_scraping(event, supabase_url, supabase_headers):
    """Handle the asynchronous scraping execution"""
    try:
        scraping_attempt_id = event['scrapingAttemptId']
        place_ids = event['placeIds']
        max_reviews = event['maxReviews']
        reviews_start_date = event.get('reviewsStartDate')
        business_place_id = event['businessPlaceId']

        # Supabase URL and headers are passed in, but let's ensure they are valid
        if not supabase_url or not supabase_headers:
             # This case should ideally not happen if lambda_handler passes them correctly,
             # but adding a check for robustness.
             print("Supabase URL or headers missing in async handler.")
             return {
                'statusCode': 500,
                'body': json.dumps({'error': 'Supabase configuration missing for async task'})
            }


        return handle_synchronous_scraping(\
            scraping_attempt_id, place_ids, max_reviews, reviews_start_date,\
            business_place_id, supabase_url, supabase_headers\
        )

    except Exception as error:
        print(f"Async scraping error: {traceback.format_exc()}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': 'Async scraping failed', 'details': str(error)})\
        }

def handle_synchronous_scraping(scraping_attempt_id, place_ids, max_reviews, reviews_start_date, business_place_id, supabase_url, supabase_headers, user_profile_id=None):
    """Handle the actual scraping work"""
    try:
        # Check Apify token - Fetch from environment variable
        apify_token = os.environ.get("APIFY_TOKEN")
        if not apify_token:
            error_msg = 'Apify token not configured in environment variables'
            try:
                # Use passed in supabase details for updating metadata
                if supabase_url and supabase_headers:
                    requests.patch(
                        f'{supabase_url}/rest/v1/scraping_metadata?id=eq.{scraping_attempt_id}',
                        headers=supabase_headers,
                        data=json.dumps({
                            'scraping_status': 'failed',
                            'error_message': error_msg
                        }),
                        timeout=30
                    ).raise_for_status()
                else:
                     print("Supabase details missing, cannot update metadata for Apify token error.")
            except Exception as update_error:
                print(f"Failed to update metadata for Apify token error: {str(update_error)}")

            return {
                'statusCode': 500,
                'headers': CORS_HEADERS,
                'body': json.dumps({'error': error_msg})
            }

        # Initialize Apify client and run scraping
        try:
            client = ApifyClient(apify_token)
            run_input = {
                'placeIds': place_ids,
                'maxReviews': max_reviews,
                'reviewsSort': 'newest',
                'language': 'en',
                'reviewsOrigin': 'all',
                'personalData': True,
            }

            if reviews_start_date and reviews_start_date.strip():
                run_input['reviewsStartDate'] = reviews_start_date

            run = client.actor("Xb8osYTtOjlsgI6k9").call(run_input=run_input)

        except Exception as e:
            error_msg = f"Apify scraping failed: {str(e)}"
            try:
                 # Use passed in supabase details for updating metadata
                if supabase_url and supabase_headers:
                    requests.patch(
                        f'{supabase_url}/rest/v1/scraping_metadata?id=eq.{scraping_attempt_id}',
                        headers=supabase_headers,
                        data=json.dumps({
                            'scraping_status': 'failed',
                            'error_message': error_msg
                        }),
                        timeout=30
                    ).raise_for_status()
                else:
                    print("Supabase details missing, cannot update metadata for Apify call error.")
            except Exception as update_error:
                print(f"Failed to update metadata for Apify call error: {str(update_error)}")

            return {
                'statusCode': 500,
                'headers': CORS_HEADERS,
                'body': json.dumps({'error': 'Scraping failed', 'details': error_msg})
            }

        # Process results
        processed_results = {
            'totalItems': 0,
            'successfulInserts': 0,
            'errors': [],
            'businessInfo': None,
            'scrapingAttemptId': scraping_attempt_id,
        }

        total_reviews_scraped = 0
        scraping_errors = []

        try:
            items = list(client.dataset(run["defaultDatasetId"]).iterate_items())
            processed_results['totalItems'] = len(items)
        except Exception as e:
            error_msg = f"Failed to retrieve scraped data: {str(e)}"
            try:
                 # Use passed in supabase details for updating metadata
                if supabase_url and supabase_headers:
                    requests.patch(
                        f'{supabase_url}/rest/v1/scraping_metadata?id=eq.{scraping_attempt_id}',
                        headers=supabase_headers,
                        data=json.dumps({
                            'scraping_status': 'failed',
                            'error_message': error_msg
                        }),
                        timeout=30
                    ).raise_for_status()
                else:
                    print("Supabase details missing, cannot update metadata for dataset retrieval error.")
            except Exception as update_error:
                print(f"Failed to update metadata for dataset retrieval error: {str(update_error)}")

            return {
                'statusCode': 500,
                'headers': CORS_HEADERS,
                'body': json.dumps({'error': 'Failed to retrieve scraped data', 'details': error_msg})
            }

        # Process each item
        for item in items:
            try:
                if not business_place_id and item.get('placeId'):
                    business_place_id = item['placeId']

                if item.get('error'):
                    error_info = ProcessedError(
                        type=item['error'],
                        description=item.get('errorDescription', item['error']),
                        place_id=item.get('placeId')
                    )
                    if item['error'] == 'no_reviews':
                        error_info.business_info = {
                            'title': item.get('title'),
                            'address': item.get('address'),
                            'totalScore': item.get('totalScore'),
                            'reviewsCount': item.get('reviewsCount'),
                        }
                        processed_results['businessInfo'] = ProcessedBusinessInfo(
                            place_id=item.get('placeId'),
                            title=item.get('title'),
                            category_name=item.get('categoryName'),
                            categories=','.join(item.get('categories', [])),
                            address=item.get('address'),
                            neighborhood=item.get('neighborhood'),
                            street=item.get('street'),
                            city=item.get('city'),
                            postal_code=item.get('postalCode'),
                            state=item.get('state'),
                            country_code=item.get('countryCode'),
                            location=item.get('location'),
                            total_score=item.get('totalScore'),
                            reviews_count=item.get('reviewsCount'),
                            price=item.get('price'),
                            permanently_closed=item.get('permanentlyClosed'),
                            temporarily_closed=item.get('temporarilyClosed'),
                            image_url=item.get('imageUrl'),
                            url=item.get('url'),
                            cid=item.get('cid'),
                            fid=item.get('fid'),
                        ).__dict__
                    processed_results['errors'].append(error_info.__dict__)
                    scraping_errors.append(item.get('errorDescription', item['error']))
                    continue

                # Process review data
                try:
                    cleaned_item = clean_review_data(item)

                    try:
                        review_json = json.dumps({'review_data': cleaned_item})
                    except (TypeError, ValueError) as json_error:
                        raise Exception(f"Review data is not JSON serializable even after cleaning: {str(json_error)}")

                    response = requests.post(
                        f'{supabase_url}/rest/v1/rpc/process_google_review',
                        headers=supabase_headers,
                        data=review_json,
                        timeout=30
                    )

                    response.raise_for_status()

                    try:
                        result = response.json()
                    except json.JSONDecodeError as json_err:
                        raise Exception(f"Invalid JSON response from database: {response.text[:500]}")

                    if result:
                        processed_results['successfulInserts'] += 1
                        total_reviews_scraped += 1

                        # Handle different response formats
                        review_id = None
                        if isinstance(result, list) and len(result) > 0:
                            if isinstance(result[0], dict) and result[0].get('id'):
                                review_id = result[0]['id']
                        elif isinstance(result, dict) and result.get('id'):
                            review_id = result['id']

                        # Update the review with scraping attempt ID if we have a review ID
                        if review_id:
                            try:
                                check_review = requests.get(
                                    f"{supabase_url}/rest/v1/reviews?id=eq.{review_id}",
                                    headers=supabase_headers,
                                    timeout=30
                                )
                                if check_review.ok and check_review.json():
                                    requests.patch(
                                        f'{supabase_url}/rest/v1/reviews?id=eq.{review_id}',
                                        headers=supabase_headers,
                                        data=json.dumps({'scraping_attempt': scraping_attempt_id}),
                                        timeout=30
                                    ).raise_for_status()
                                else:
                                    print(f"Skipping patch: review {review_id} not found")
                            except requests.exceptions.RequestException as patch_error:
                                print(f"Failed to update review {review_id} with scraping attempt: {patch_error}")

                except requests.exceptions.RequestException as db_error:
                    error_msg = f"Database error: {str(db_error)}"
                    if hasattr(db_error, 'response') and db_error.response is not None:
                        try:
                            error_msg += f" Response: {db_error.response.text}"
                        except:
                            pass

                    error_info = ProcessedError(
                        type='database_error',
                        description=error_msg,
                        review_id=item.get('reviewId')
                    )
                    processed_results['errors'].append(error_info.__dict__)
                    scraping_errors.append(error_msg)

            except Exception as item_error:
                error_type = type(item_error).__name__
                error_msg = str(item_error) if str(item_error) != "0" else "Unknown error"
                item_traceback = traceback.format_exc()

                detailed_error = f"{error_type}: {error_msg}"
                if error_msg == "Unknown error" or not error_msg:
                    detailed_error = f"Exception in item processing: {item_traceback}"

                print(f"Item processing error for review {item.get('reviewId', 'unknown')}: {item_traceback}")

                error_info = ProcessedError(
                    type='processing_error',
                    description=detailed_error,
                    review_id=item.get('reviewId')
                )
                processed_results['errors'].append(error_info.__dict__)
                scraping_errors.append(detailed_error)

        # Update final status
        final_status = 'completed' if not scraping_errors else ('failed' if total_reviews_scraped == 0 else 'completed_with_errors')
        try:
             # Use passed in supabase details for updating metadata
            if supabase_url and supabase_headers:
                requests.patch(
                    f'{supabase_url}/rest/v1/scraping_metadata?id=eq.{scraping_attempt_id}',
                    headers=supabase_headers,
                    data=json.dumps({
                        'business_place_id': business_place_id,
                        'scraping_status': final_status,
                        'total_reviews_scraped': total_reviews_scraped,
                        'error_message': '; '.join(scraping_errors[:5]) if scraping_errors else None,
                    }),
                    timeout=30
                ).raise_for_status()
            else:
                print("Supabase details missing, cannot update final metadata status.")
        except requests.exceptions.RequestException as e:
            print(f"Failed to update scraping metadata: {str(e)}")

        # When you create a business (example):
        business_payload = {
            'place_id': business_place_id
        }
        if user_profile_id is not None:
            business_payload['user_profile'] = int(user_profile_id)

        # Example POST request to create the business (add this where business is created):
        try:
            if supabase_url and supabase_headers:
                response = requests.post(
                    f'{supabase_url}/rest/v1/businesses',
                    headers=supabase_headers,
                    data=json.dumps(business_payload),
                    timeout=30
                )
                response.raise_for_status()
            else:
                print("Supabase details missing, cannot create business record.")
        except requests.exceptions.RequestException as e:
            print(f"Failed to create business record: {str(e)}")


        return {
            'statusCode': 200,
            'headers': CORS_HEADERS,
            'body': safe_json_dumps(processed_results)
        }

    except Exception as error:
        error_msg = f"{type(error).__name__}: {str(error)}"
        print(f"Synchronous scraping error: {traceback.format_exc()}")

        try:
             # Use passed in supabase details for updating metadata on error
            if supabase_url and supabase_headers:
                requests.patch(
                    f'{supabase_url}/rest/v1/scraping_metadata?id=eq.{scraping_attempt_id}',
                    headers=supabase_headers,
                    data=json.dumps({
                        'scraping_status': 'failed',
                        'error_message': error_msg
                    }),
                    timeout=30
                ).raise_for_status()
            else:
                print("Supabase details missing, cannot update metadata on synchronous error.")
        except Exception as update_error:
            print(f"Failed to update metadata on synchronous error: {str(update_error)}")


        return {
            'statusCode': 500,
            'headers': CORS_HEADERS,
            'body': json.dumps({'error': 'Scraping failed', 'details': error_msg})
        }

def handle_status_check(event):
    """Handle status check requests"""
    try:
        # Extract scraping attempt ID from path
        path = event.get('path', '')
        scraping_attempt_id = path.split('/')[-1]

        # Database configuration - Fetch from environment variables
        supabase_url =os.getenv("SUPABASE_URL")
        supabase_key =os.getenv("SUPABASE_KEY")

        if not supabase_url or not supabase_key:
             return {
                'statusCode': 500,
                'headers': CORS_HEADERS,
                'body': json.dumps({'error': 'Supabase URL or Key not configured for status check'})
            }

        supabase_headers = {
            'apikey': supabase_key,
            'Authorization': f'Bearer {supabase_key}',
            'Content-Type': 'application/json'
        }

        response = requests.get(
            f'{supabase_url}/rest/v1/scraping_metadata?id=eq.{scraping_attempt_id}',
            headers=supabase_headers,
            timeout=30
        )
        response.raise_for_status()

        data = response.json()
        if not data:
            return {
                'statusCode': 404,
                'headers': CORS_HEADERS,
                'body': json.dumps({'error': 'Scraping attempt not found'})
            }

        return {
            'statusCode': 200,
            'headers': CORS_HEADERS,
            'body': json.dumps(data[0])
        }

    except Exception as error:
        return {
            'statusCode': 500,
            'headers': CORS_HEADERS,
            'body': json.dumps({'error': 'Failed to check status', 'details': str(error)})
        }









if __name__ == '__main__':
    print("index.py started from CLI...")

    # Simulate a test Lambda event
    fake_event = {
        'httpMethod': 'POST',
        'body': json.dumps({
            'placeIds': ['ChIJN1t_tDeuEmsRUsoyG83frY4'],
            'scraping_metadata_id': 'demo-test-id',
            'maxReviews': 2,
            'returnImmediately': False
        }),
    }

    result = lambda_handler(fake_event, context={})
    print("Lambda handler result:")
    print(result)
