import datetime

def generate_showid(theaterid, showdate):
	t = theaterid.strip('theater')
	s = showdate.replace('-','')
	showid = "show"+t+"_"+s
	return showid

def generate_showid_with_timeslot(theaterid, showdate, timeslotid):
    t = theaterid.strip('theater')
    s = showdate.replace('-','')
    ts = timeslotid.strip('timeslot')
    showid = "show"+t+"_"+s+"_"+ts
    return showid
    

def str_to_date(date):
	return datetime.datetime.strptime(date, '%Y-%m-%d')