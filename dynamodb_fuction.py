import boto3
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key, Attr
import bms_util
import random


client = boto3.resource('dynamodb')
table = client.Table('BookMyShow')

def get_available_genres():
    
    response = table.query(
        IndexName="SK-PK-index",
        KeyConditionExpression=Key('SK').eq('genre')
        )
    genres_dic = {}
    for i in response['Items']:
        genres_dic[i['GenreName'].lower()] = i['PK']
    return genres_dic
    
    
def get_available_movies(genreid):
    response = table.query(
        IndexName="GSI1-SK-index",
        KeyConditionExpression=Key('GSI1').eq(genreid) & Key('SK').begins_with('moviegenre')
    )
    movies_dic = {}
    
    for i in response['Items']:
        
        movie = table.get_item(Key={'PK': i['PK'], 'SK': 'movie'})['Item']
        movies_dic[movie['GSI1']] = movie['PK']
        
    return movies_dic
    
    
def get_available_theaters(movieid):
    response = table.query(
        KeyConditionExpression=Key('PK').eq(movieid) & Key('SK').begins_with('theater')
    )
    
    theaters_dic = {}
    
    for i in response['Items']:
        theater = table.get_item(Key={'PK': i['SK'], 'SK': 'theater'})['Item']
        theaters_dic[theater['GSI1']] = theater['PK']
        
    return theaters_dic
    
def is_movie_showing_in_theater(movieid, theaterid, date):
    response = table.get_item(Key={'PK': movieid, 'SK': theaterid})
    startdate = bms_util.str_to_date(response['Item']['show_from_date'])
    enddate = bms_util.str_to_date(response['Item']['show_to_date'])
    date = bms_util.str_to_date(date)
    if startdate <= date <= enddate:
        return True
    else:
        return False
    
def is_movie_available_on_date(movieid, theaterid, date):
    showid = bms_util.generate_showid(theaterid, date)
    response = table.query(
        IndexName="GSI1-SK-index",
        KeyConditionExpression=Key('GSI1').eq(movieid),
        FilterExpression= Attr('PK').begins_with(showid)
    )
        
    if len(response['Items']) == 0:
        status = False
        if is_movie_showing_in_theater(movieid, theaterid, date):
            timeslots = get_available_timeslots_to_assign_movie(theaterid, date)
            if len(timeslots) == 0:
                return status
            else:
                status= generate_shows(movieid, theaterid, date, timeslots)
                return status
        else:
            return False
    else:
        return True
        
def get_available_timeslots(theaterid, date, seats):
    
    timeslot_dic = {}
    response =table.query(
        IndexName="SK-PK-index",
        KeyConditionExpression=Key('SK').eq('timeslot')
        )
        
    for timeslot in response['Items']:
        showid = bms_util.generate_showid_with_timeslot(theaterid, date, timeslot['PK'])
        res = table.query(
            KeyConditionExpression= Key('PK').eq(showid),
            FilterExpression=Attr('status').eq('available')
        )
        
        if len(res['Items']) >= seats:
            timeslot_dic[timeslot['GSI1']] = timeslot['PK']
            
    return timeslot_dic
    
    
def get_available_seatrows(theaterid, date, timeslotid, seats):
    available_seatrows = []
    showid = bms_util.generate_showid_with_timeslot(theaterid, date, timeslotid)
    response = table.query(
            KeyConditionExpression= Key('PK').eq(showid),
            FilterExpression=Attr('status').eq('available')
        )
    
    seatrows = []
    
    for r in response['Items']:
        seatrows.append(r['SeatRow'])
        
    seatrow_count = [(x, seatrows.count(x)) for x in set(seatrows)]
    
    for e in seatrow_count:
        if e[1] >= seats:
            available_seatrows.append(e[0])
            
    return available_seatrows
    
    
def get_available_seatnumbers(theaterid, date, timeslotid, seatrow):
    available_seatnumbers = []
    
    showid = bms_util.generate_showid_with_timeslot(theaterid, date, timeslotid)
    
    response = table.query(
            KeyConditionExpression= Key('PK').eq(showid),
            FilterExpression=Attr('status').eq('available') & Attr('SeatRow').eq(seatrow.upper())
        )
    
    for r in response['Items']:
        available_seatnumbers.append(r['SeatNum'])
    
    return  available_seatnumbers
    
    
def book_ticket(theaterid, date, timeslotid, seatrow, inputseats):
    showid = bms_util.generate_showid_with_timeslot(theaterid, date, timeslotid)
    seatids =  ['seat_{}{}'.format(seatrow.lower(),seat) for seat in inputseats]
    
    status = False
    for seatid in seatids:
        status = book(showid, seatid)
    
    return status

def book(showid, seatid):
    
    try:
        response = table.update_item(
            Key={
                'PK': showid,
                'SK': seatid
            },
            UpdateExpression='SET #attr1 = :val1',
            ExpressionAttributeNames={'#attr1': 'status'},
            ExpressionAttributeValues={':val1': 'booked'}
        )
        return True
    except:
        return False
        
def get_available_timeslots_to_assign_movie(theaterid, date):
    showid = bms_util.generate_showid(theaterid, date)
    timeslots = []
    res = table.query(
        IndexName="SK-PK-index",
        KeyConditionExpression=Key('SK').eq(theaterid) & Key('PK').begins_with(showid)
    )
    
    for t in res['Items']:
        timeslots.append(t['showtime'])
        
    return list(set(['timeslot09151100','timeslot12151400','timeslot15151700','timeslot18152000','timeslot21152300']) - set(timeslots))
    
    
def write_items_to_db(items):
    try:
        with table.batch_writer() as batch:
            for i in items:
                batch.put_item(Item=i)
        return True
    except:
        return False
        
        
def generate_shows(movieid, theaterid, date, timeslots):
    shows = []
    show_movie_joins = []
    
    if len(timeslots) > 3:
        timeslots = random.sample(timeslots, 3)
    seatrows = ["A","B","C","D","E","F","G","H","I","J","K","L"]
    seatnums = ["1","2","3","4","5","6","7","8","9","10"]
    
    for t in timeslots:
        th = theaterid.strip('theater')
        ts = t.strip('timeslot')
        d = date.replace('-','')
        showid = "show"+th+'_'+d+'_'+ts
        
        show_movie_joins.append({"PK":showid, "SK": theaterid, "GSI1":movieid, "showtime":t})
        for sr in seatrows:
            for sn in seatnums:
                seatid = "seat_"+sr.lower()+sn
                shows.append({"PK":showid, "SK": seatid, "SeatRow":sr, "SeatNum":sn, "status":"available"})
    
    status = write_items_to_db(shows)
    status = write_items_to_db(show_movie_joins)
    return status