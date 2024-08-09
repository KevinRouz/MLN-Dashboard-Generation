#this file contains a class named rules for mln generation. If the attribute selected by the user for each layer generation is a valid combination then 
#if the atrribute values chosen by the user is not null for the following input combination then the inputype is valid
from enum import IntEnum

class ErrorType(IntEnum):
    INPUT_FILE_NAME=1
    LAYER_GENERATION_TYPE=2
    PRIMARY_KEY_COLUMN=3
    primary_key_converted_filename=4
    FEATURE_COLUMN=5
    FEATURE_TYPE=6
    SIMILARITY_METRIC=7
    THRESHOLD=8
    RANGE=9
    MULTI_RANGE=10
    NUMBER_OF_EQUI_SIZED_SEGMENTS=11
    LONGITUDE_FEATURE_COLUMN=12
    LATITUDE_FEATURE_COLUMN=13
    DATE_METRIC=14
    DATE_FORMAT=15
    TIME_FORMAT=16
    INTER_LAYER_GENERATION_TYPE=17 
    LAYER_1_INPUT_FILE_NAME=18
    LAYER_2_INPUT_FILE_NAME=19
    JOIN_COLUMN_NAME=20
    INCORRECT_INPUT_ATTRIBUTE=21
    
    #RELATIONSHIP_NAME=29
    #LAYER_NAME=2
    #NODE_NUMBER=18
    #EDGE_NUMBER=19
    #CON_COM_NO=20
    #SYSTEM_TIME=21
    #INTER_LAYER_NAME=22
    #LAYER_2_NAME=26
    #LAYER_1_NAME=24
