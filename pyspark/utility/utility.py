import math
import json
import subprocess
from datetime import datetime

def determine_time_slot(time):
    """
    determines time slot of the day based on given datetime
    :type time: str     string containing datetime "yyyy-mm-dd hh:mm:ss"
    :rtype    : int     id of the 10-minute slot, 0 <= id < 144
    """
    dt = datetime.strptime(time, "%Y-%m-%d %H:%M:%S")
    return (dt.hour*60+dt.minute)/10

def determine_block_ids(lon, lat):
    """
    calculates ids of blocks/subblocks based on given coordinates
    :type lon: float            longitude
    :type lat: float            latitude
    :rtype : [(int, int),       list of two tuples which contain x and y ids
              (int, int)]       of large and small block, respectively
    """
    # size of large block is 0.005   degree lat/lon
    # size of small block is 0.00025 degree lat/lon
    corner = [(lon+74.25), (lat-40.5)]

    small_block_id = map(lambda x: int(math.floor(x/0.00025)), corner)
    large_block_id = map(lambda x: x/20, small_block_id)

    return tuple(large_block_id), tuple(small_block_id)

def get_neighboring_blocks(bl):
    """
    returns list of block_id for blocks that are adjacent to block bl
    :type bl: (int, int)        x and y ids for large block bl
    :rtype  : list[(int, int)]  list of x and y ids for large blocks surrounding block bl
    """
    return [(bl[0]+i, bl[1]+j) for i in [-1,0,+1] for j in [-1,0,+1] if not (i == 0 and j == 0)]

def determine_subblock_lonlat(subblock):
    """
    calculates coordinates of the center of a subblock based on block_id and subblock_id
    :type subblock: (int, int)       x and y ids of small block subblock
    :rtype        : [float, float]   longitude and latitude of the small block center
    """
    corner = (-74.25, 40.5)
    return [corner[i]+(subblock[i]+0.5)*0.00025 for i in range(2)]

def map_schema(line, schema):
    """
    cleans the message msg, leaving only the fields given by schema, and casts the appropriate types
    returns None if unable to parse
    :type line  : str       message to parse
    :type schema: dict      schema that contains the fields to filter
    :rtype      : dict      message in the format {"field": value}
    """
    try:
        msg = line.split(schema["DELIMITER"])
        msg = {key:eval("%s(\"%s\")" % (schema["FIELDS"][key]["type"],
                                    msg[schema["FIELDS"][key]["index"]]))
                        for key in schema["FIELDS"].keys()}
    except:
        return
    return msg

def add_block_fields(record):
    """
    adds fields block_id ((int, int)), sub_block_id ((int, int)), block_latid (int), block_lonid (int)
    to the record based on existing fields longitude and latitude
    returns None if unable to add fields
    :type record: dict      record into which insert new fields
    :rtype      : dict      record with inserted new fields
    """
    try:
        lon, lat = [record[field] for field in ["longitude", "latitude"]]
        record["block_id"], record["sub_block_id"] = determine_block_ids(lon, lat)
        record["block_latid"], record["block_lonid"] = record["block_id"]
    except:
        return
    return dict(record)

def add_time_slot_field(record):
    """
    adds field time_slot (int) to the record based on existing field datetime
    returns None if unable to add field
    :type record: dict      record into which insert new field
    :rtype      : dict      record with inserted new field
    """
    try:
        record["time_slot"] = determine_time_slot(record["datetime"])
    except:
        return
    return dict(record)

def check_passengers(record):
    """
    returns None if number of passengers is < 1 or if unable to convert field
    :type record: dict      record where to convert field
    :rtype      : dict      record with converted field
    """
    try:
        number = record["passengers"]
    except:
        return
    if number < 1:
        return
    return dict(record)

def parse_config(configfile):
    """
    reads configs saved as json record in configuration file and returns them
    :type configfile: str       path to config file
    :rtype          : dict      configs
    """
    conf = json.load(open(configfile, "r"))
    return replace_envvars_with_vals(conf)

def replace_envvars_with_vals(dic):
    """
    for a dictionary dic which may contain values of the form "$varname",
    replaces such values with the values of corresponding environmental variables
    :type dic: dict     dictionary where to parse environmental variables
    :rtype   : dict     dictionary with parsed environmental variables
    """
    for el in dic.keys():
        val = dic[el]
        if type(val) is dict:
            val = replace_envvars_with_vals(val)
        else:
            if type(val) in [str] and len(val) > 0 and '$' in val:
                command = "echo {}".format(val)
                dic[el] = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE).stdout.read().strip()
    return dic