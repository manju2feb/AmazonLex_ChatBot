import json
import datetime
import time
import os
import dateutil.parser
import logging
import boto3
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key, Attr
import dynamodb_function

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

client = boto3.resource('dynamodb')

#initializing global variable with dummie data
MOVIE_ID =''
THEATER_ID= ''
TIMESLOTID=''
AVAILABLE_MOVIES=['Th Dark Knight']
AVAILABLE_THEATERS={'theater101':'Fox Theater'}
AVAILABLE_TIMESLOTS={"timeslot09151100":"9:15 AM - 11:00 AM"}
AVAILABLE_SEATROWS=['A','B']
AVAILABLE_SEATNUMBERS=['1','2','3']
INPUT_SEATNUMBERS=['1','2']


# --- Helpers that build all of the responses ---


def elicit_slot(session_attributes, intent_name, slots, slot_to_elicit, message):
    return {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'ElicitSlot',
            'intentName': intent_name,
            'slots': slots,
            'slotToElicit': slot_to_elicit,
            'message': message
        }
    }


def elicit_slot_with_card(session_attributes, intent_name, slots, slot_to_elicit, message, response_card):
    return {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'ElicitSlot',
            'message': {
                'contentType': 'PlainText',
                'content': message
            },
            'intentName': intent_name,
            'slots': slots,
            'slotToElicit': slot_to_elicit,
            'responseCard': response_card
        }
    }
    
def elicit_slot_with_imagecard(session_attributes, intent_name, slots, slot_to_elicit,message, response_card):
    return {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'ElicitSlot',
            'message': {
                'contentType': 'PlainText',
                'content': message
            },
            'intentName': intent_name,
            'slots': slots,
            'slotToElicit': slot_to_elicit,
            'responseCard': response_card
        }
    }


    
def build_response_card(options):
    """
    Build a responseCard with a title, subtitle, and an optional set of options which should be displayed as buttons.
    """
    buttons = None
    if options is not None:
        buttons = []
        for i in range(min(5, len(options))):
            buttons.append(options[i])

    return {
        'contentType': 'application/vnd.amazonaws.card.generic',
        'version': 1,
        'genericAttachments': [{
            'buttons': buttons
        }]
    }
    
def build_response_card_with_image(options, title, subtitle, imagelink):
    """
    Build a responseCard with a title, subtitle, and an optional set of options which should be displayed as buttons.
    """
    buttons = None
    if options is not None:
        buttons = []
        for i in range(min(3, len(options))):
            buttons.append(options[i])

    return {
        'contentType': 'application/vnd.amazonaws.card.generic',
        'version': 1,
        'genericAttachments': [{
            'title': title,
            'subTitle': subtitle,
            'imageUrl': imagelink,
            'buttons': buttons
        }]
    }

def confirm_intent(session_attributes, intent_name, slots, message):
    return {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'ConfirmIntent',
            'intentName': intent_name,
            'slots': slots,
            'message': message
        }
    }


def close(session_attributes, fulfillment_state, message):
    return {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'Close',
            'fulfillmentState': fulfillment_state,
            'message': message
        }
    }


def delegate(session_attributes, slots):
    return {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'Delegate',
            'slots': slots
        }
    }


# --- Helper Functions ---


def safe_int(n):
    """
    Safely convert n value to int.
    """
    if n is not None:
        return int(float(n))
    return n


def try_ex(func):
    """
    Call passed in function in try block. If KeyError is encountered return None.
    This function is intended to be used to safely access dictionary.

    Note that this function would have negative impact on performance.
    """

    try:
        return func()
    except KeyError:
        return None




def isvalid_genre(genre):
    global AVAILABLE_MOVIES
    valid_genres = dynamodb_function.get_available_genres()
    if genre.lower() in list(valid_genres.keys()):
        genreid = valid_genres[genre.lower()]
        AVAILABLE_MOVIES = dynamodb_function.get_available_movies(genreid)
        return True
    else:
        return False
        

def isvalid_movie(moviename):
    global AVAILABLE_THEATERS
    global MOVIE_ID
    if moviename.title() in AVAILABLE_MOVIES.keys():
        MOVIE_ID = AVAILABLE_MOVIES[moviename.title()]
        AVAILABLE_THEATERS = dynamodb_function.get_available_theaters(MOVIE_ID)
        return True
    else:
        return False
    
    
def isvalid_theater(theater):
    global THEATER_ID
    if theater.title() in AVAILABLE_THEATERS.keys():
        THEATER_ID = AVAILABLE_THEATERS[theater.title()]
        return True
    else:
        return False
        

def isvalid_seat_row(seatrow):
    return seatrow.upper() in AVAILABLE_SEATROWS
    
def isvalid_date(date):
    try:
        dateutil.parser.parse(date)
        return True
    except ValueError:
        return False
        
def isvalid_int(n):
    try:
        float(n)
    except ValueError:
        return False
    else:
       return float(n).is_integer()

def isvalid_timeslot(timeslot):
    global TIMESLOTID
    if timeslot in list(AVAILABLE_TIMESLOTS.keys()):
        TIMESLOTID = AVAILABLE_TIMESLOTS[timeslot]
        return True
    else:
        return False

def isvalid_seatnumbers(seatnum):
    global INPUT_SEATNUMBERS
    sn = seatnum.split(' ')
    if all(s in AVAILABLE_SEATNUMBERS for s in sn):
        INPUT_SEATNUMBERS = sn
        return True
    else:
        return False

def build_validation_result(isvalid, violated_slot, message_content):
    return {
        'isValid': isvalid,
        'violatedSlot': violated_slot,
        'message': {'contentType': 'PlainText', 'content': message_content}
    }
    
    
def validate_booking(slots):
    
    movie_genre = try_ex(lambda: slots['MovieGenre'])
    movie_name = try_ex(lambda: slots['MovieName'])
    theater_name = try_ex(lambda: slots['TheaterName'])
    movie_date = try_ex(lambda: slots['MovieDate'])
    ticket_count = try_ex(lambda: slots['TicketCount'])
    movie_time = try_ex(lambda: slots['MovieTime'])
    seat_row = try_ex(lambda: slots['SeatRow'])
    seat_numbers = try_ex(lambda: slots['SeatNumbers'])
    
    if movie_genre:
        if not isvalid_genre(movie_genre):
            return build_validation_result(
                False,
                'MovieGenre',
                'No such movie genre like {}.Please choose a valid genre. e.g. action, fantasy, horror, adventure etc.'.format(movie_genre)
            )
        if len(AVAILABLE_MOVIES) == 0:
            return build_validation_result(
                False,
                'MovieGenre',
                'No any {} movie available.Please try any other genre.'.format(movie_genre)
            )

    if movie_name:
        if not isvalid_movie(movie_name):
            return build_validation_result(
                False,
                'MovieName',
                'There is not such movie like {} or Showtime is not available. You can choose from currently available movies.'.format(movie_name)
            )
        if len(AVAILABLE_THEATERS) ==0:
            return build_validation_result(
                False,
                'MovieName',
                'No any available theaters.Please try any other movie.'
            )
            
        
    if theater_name and not isvalid_theater(theater_name):
        return build_validation_result(
            False,
            'TheaterName',
            'Showtime in theater {} is not available. You can choose one from the list above.'.format(theater_name.title())
        )
    
    if movie_date:
        if not isvalid_date(movie_date):
            return build_validation_result(False, 'MovieDate', 'I did not understand date.  When would you like to watch your movie?')
        if datetime.datetime.strptime(movie_date, '%Y-%m-%d').date() < datetime.date.today():
            return build_validation_result(False, 'MovieDate', 'Booking must be scheduled at least one day in advance.  Can you try a different date?')
        if datetime.datetime.strptime(movie_date, '%Y-%m-%d').date() >= datetime.date.today()+datetime.timedelta(days=30):
            return build_validation_result(False, 'MovieDate', 'I can book only upto 1 month in advance. Can you try a date between {} and {}?'.format(datetime.date.today(),datetime.date.today()+datetime.timedelta(days=30)))
        if not dynamodb_function.is_movie_available_on_date(MOVIE_ID, THEATER_ID, movie_date):
            return build_validation_result(False, 'MovieDate', 'Movie not showing on {}. please choose another date.'.format(movie_date))
            
    if ticket_count:
        if not isvalid_int(ticket_count) or safe_int(ticket_count) < 1:
            return build_validation_result(
                False,
                'TicketCount',
                'Invalid input! Enter a valid number of tickets you would like to book.'
            )
        if safe_int(ticket_count) > 10:
            return build_validation_result(
                False,
                'TicketCount',
                'You can book Max 10 tickets using the chatbot.How many tickets would you like to book?'
            )
 
        ticket_count = safe_int(ticket_count)
        global AVAILABLE_TIMESLOTS
        AVAILABLE_TIMESLOTS = dynamodb_function.get_available_timeslots(THEATER_ID, movie_date, ticket_count)
        if len(AVAILABLE_TIMESLOTS) == 0:
            return build_validation_result(
            False,
            'TicketCount',
            'No {} tickets available in any time slots.'.format(ticket_count)
            )
        
    if movie_time:
        if not isvalid_timeslot(movie_time):
            return build_validation_result(
                False,
                'MovieTime',
                'Invalide time slots.Please choose from available time slots.'
        )
        global AVAILABLE_SEATROWS
        AVAILABLE_SEATROWS = dynamodb_function.get_available_seatrows(THEATER_ID, movie_date, TIMESLOTID, ticket_count)
            
        
    if seat_row:
        if not isvalid_seat_row(seat_row):
            return build_validation_result(
                False,
                'SeatRow',
                'Invalid input seat row. You can choose from currently available Rows. {}'.format(', '.join(sorted(AVAILABLE_SEATROWS)))
            )
        else:
            global AVAILABLE_SEATNUMBERS
            AVAILABLE_SEATNUMBERS = dynamodb_function.get_available_seatnumbers(THEATER_ID, movie_date, TIMESLOTID, seat_row)
            
    if seat_numbers:
        if not isvalid_seatnumbers(seat_numbers):
            return build_validation_result(
                False,
                'SeatNumbers',
                'Invalid seat numbers input. You can choose from currently available seats seperated by space. {}'.format(sorted([int(i) for i in AVAILABLE_SEATNUMBERS]))
            )
        if len(INPUT_SEATNUMBERS) > ticket_count or len(INPUT_SEATNUMBERS) < ticket_count:
            return build_validation_result(
                False,
                'SeatNumbers',
                'Inefficient input. Please enter {} seats from currently available seats seperated by space. {}'.format(ticket_count, sorted([int(i) for i in AVAILABLE_SEATNUMBERS]))
            )
        
        

    return {'isValid': True}
    
def build_options(slot):
    """
    Build a list of potential options for a given slot, to be used in responseCard generation.
    """
    if slot == 'MovieName':
        movies = []
        for movie in AVAILABLE_MOVIES.keys():
            movies.append({'text': movie, 'value': movie})
        return movies
        
    if slot == 'TheaterName':
        theaters = []
        for theater in AVAILABLE_THEATERS.keys():
            theaters.append({'text': theater, 'value': theater})
        return theaters

        
    if slot == 'MovieTime':
        timeslots = []
        for key in sorted(AVAILABLE_TIMESLOTS.keys()):
            timeslots.append({'text': key, 'value': key})
        return timeslots


def book_movie(intent_request):
    
    movie_genre = try_ex(lambda: intent_request['currentIntent']['slots']['MovieGenre'])
    movie_name = try_ex(lambda: intent_request['currentIntent']['slots']['MovieName'])
    theater_name = try_ex(lambda: intent_request['currentIntent']['slots']['TheaterName'])
    movie_date = try_ex(lambda: intent_request['currentIntent']['slots']['MovieDate'])
    ticket_count = try_ex(lambda: intent_request['currentIntent']['slots']['TicketCount'])
    movie_time = try_ex(lambda: intent_request['currentIntent']['slots']['MovieTime'])
    seat_row = try_ex(lambda: intent_request['currentIntent']['slots']['SeatRow'])
    seat_numbers = try_ex(lambda: intent_request['currentIntent']['slots']['SeatNumbers'])
    
    confirmation_status = intent_request['currentIntent']['confirmationStatus']
    session_attributes = intent_request['sessionAttributes'] if intent_request['sessionAttributes'] is not None else {}
    
    # Load confirmation history and track the current reservation.
    booking = json.dumps({
        'ReservationType': 'Movie',
        'MovieGenre': movie_genre,
        'MovieName': movie_name,
        'TheaterName': theater_name,
        'MovieDate': movie_date,
        'TicketCount': ticket_count,
        'MovieTime': movie_time,
        'SeatRow': seat_row,
        'SeatNumbers': seat_numbers
    })


    
    if intent_request['invocationSource'] == 'DialogCodeHook':
        
        slots = intent_request['currentIntent']['slots']
        
        # Validate any slots which have been specified.  If any are invalid, re-elicit for their value
        validation_result = validate_booking(intent_request['currentIntent']['slots'])
        if not validation_result['isValid']:
            slots[validation_result['violatedSlot']] = None
            
            return elicit_slot(
                session_attributes,
                intent_request['currentIntent']['name'],
                slots,
                validation_result['violatedSlot'],
                validation_result['message']
            )
            
        if movie_genre and not movie_name:
            return elicit_slot_with_imagecard(
                session_attributes,
                intent_request['currentIntent']['name'],
                intent_request['currentIntent']['slots'],
                'MovieName',
                'Which {} movie would you like to book?'.format(movie_genre),
                build_response_card_with_image(
                    build_options('MovieName'),
                    'SPECIFY MOVIE NAME',
                    'Please choose from the below available movies.',
                    'https://images.unsplash.com/photo-1485846234645-a62644f84728?ixlib=rb-1.2.1&ixid=eyJhcHBfaWQiOjEyMDd9&auto=format&fit=crop&w=740&q=80'
                )
            )
            
        if movie_name and not theater_name:
            return elicit_slot_with_imagecard(
                session_attributes,
                intent_request['currentIntent']['name'],
                intent_request['currentIntent']['slots'],
                'TheaterName',
                "Where would you like to watch the '{}'".format(movie_name.title()),
                build_response_card_with_image(
                    build_options('TheaterName'),
                    'SPECIFY THEATER NAME',
                    'Please choose from the below available theaters.',
                    'https://images.unsplash.com/photo-1574267432644-f410f8ec2474?ixlib=rb-1.2.1&ixid=eyJhcHBfaWQiOjEyMDd9&auto=format&fit=crop&w=889&q=80'
                )
            )
                
        if ticket_count and not movie_time:
            return elicit_slot_with_imagecard(
                session_attributes,
                intent_request['currentIntent']['name'],
                intent_request['currentIntent']['slots'],
                'MovieTime',
                'When would you like to book the movie?',
                build_response_card_with_image(
                    build_options('MovieTime'),
                    'SPECIFY TIME SLOT',
                    'Please choose from the available time slots.',
                    'https://images.unsplash.com/photo-1456574808786-d2ba7a6aa654?ixlib=rb-1.2.1&ixid=eyJhcHBfaWQiOjEyMDd9&auto=format&fit=crop&w=642&q=80'
                )
            )
            
        if movie_time and not seat_row:
            return elicit_slot(
                session_attributes,
                intent_request['currentIntent']['name'],
                intent_request['currentIntent']['slots'],
                'SeatRow',
                {'contentType': 'PlainText','content': 'Please choose from the following available rows. [{}]'.format(', '.join(sorted(AVAILABLE_SEATROWS)))}
            )
            
        if seat_row and not seat_numbers:
            return elicit_slot(
                session_attributes,
                intent_request['currentIntent']['name'],
                intent_request['currentIntent']['slots'],
                'SeatNumbers',
                {'contentType': 'PlainText','content': 'Please enter your desired {} seats separated by space from row {}. {}'.format(ticket_count, seat_row.upper(),sorted([int(i) for i in AVAILABLE_SEATNUMBERS]))}
            )
            
        return delegate(session_attributes, intent_request['currentIntent']['slots'])
   
    # Booking the movie.
    if confirmation_status == 'None':
        return confirm_intent(
            session_attributes, 
            intent_request['currentIntent']['name'],
            intent_request['currentIntent']['slots'],
            {
                'contentType': 'PlainText',
                'content': "Are you sure you want to book seat '{}' for '{}' show of movie '{}' in '{}' on '{}'".format(", ".join(['{}{}'.format(seat_row.upper(),seat) for seat in INPUT_SEATNUMBERS]), movie_time, movie_name.title(),theater_name.title(), movie_date)
            }
        )
    
    if confirmation_status == 'Confirmed':
        status = 'Denied'
        msg = "Some Error occured! please try again!"
        if dynamodb_function.book_ticket(THEATER_ID, movie_date, TIMESLOTID, seat_row, INPUT_SEATNUMBERS):
            status='Fulfilled'
            msg = 'Thanks, your ticket is booked. Have a nice day!'
            
        return close(
            session_attributes,
            status,
            {
                'contentType': 'PlainText',
                'content': msg
            }
        )

    if confirmation_status == 'Denied':
        return close(
            session_attributes,
            'Failed',
            {
                'contentType': 'PlainText',
                'content': 'Okey, your ticket booking is cancelled!'
            }
        )


# --- Intents ---


def dispatch(intent_request):
    """
    Called when the user specifies an intent for this bot.
    """

    logger.debug('dispatch userId={}, intentName={}'.format(intent_request['userId'], intent_request['currentIntent']['name']))

    intent_name = intent_request['currentIntent']['name']

    # Dispatch to your bot's intent handlers
    if intent_name == 'BookMovie':
        return book_movie(intent_request)

    raise Exception('Intent with name ' + intent_name + ' not supported')


# --- Main handler ---


def lambda_handler(event, context):
    """
    Route the incoming request based on intent.
    The JSON body of the request is provided in the event slot.
    """
    # By default, treat the user request as coming from the America/New_York time zone.
    os.environ['TZ'] = 'America/New_York'
    time.tzset()
    logger.debug('event.bot.name={}'.format(event['bot']['name']))

    return dispatch(event)
