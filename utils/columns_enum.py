from enum import Enum

class ValidColumns(Enum):
    FULL_HOUSEHOLD_IDENTIFIER = "full_household_identifier" # Full household identifier.
    TIME_OF_INTERVIEW = "time_of_interview" # Time of interview in YYYY/MMM format.
    FINAL_OUTCOME_OF_SURVEY = "final_outcome_of_survey" # Final outcome of the survey.
    TYPE_OF_HOUSING_UNIT = "type_of_housing_unit" # Type of housing unit.
    HOUSEHOLD_TYPE = "household_type" # Household type.
    HOUSEHOLD_HAS_TELEPHONE = "household_has_telephone" # Apartment/Household has a telephone.
    HOUSEHOLD_CAN_ACCESS_TELEPHONE_ELSEWHERE = "household_can_access_telephone_elsewhere" # Apartment/Household can access a telephone elsewhere.
    IS_TELEPHONE_INTERVIEW_ACCEPTABLE_FOR_THE_RESPONDER = "is_telephone_interview_acceptable_for_the_responder" # Is telephone interview acceptable for the responder.
    TYPE_OF_INTERVIEW = "type_of_interview" # Type of interview.
    FAMILY_INCOME = "family_income_range" # Family income range.
    GEOGRAPHICAL_DIVISION_LOCATION = "geographical_division_location" # Geographical division/location.
    RACE = "race" # Race