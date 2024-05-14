from difflib import get_close_matches
import pandas as pd
from datetime import datetime
import mysql.connector


class DBManager:
    def __init__(self):
        pass

    def ConnectDB(self):
        return mysql.connector.connect(user='root', password='admin',
                                      host='127.0.0.1',
                                      database='patient_encounters')

    def InsertPatientRecords(self, patientRecords):
        connection = self.ConnectDB()
        cursor = connection.cursor()
        try:
            connection.autocommit = False
            for record in  patientRecords:
                patient_name = record.name
                patient_dob = str(record.dob)
                if self.ReadPatientRecord(patient_name, patient_dob)  is None:
                    query = f'INSERT INTO patients (patient_name, dob) VALUES ("{patient_name}", "{str(patient_dob)}")'
                    cursor.execute(query)
                else:
                    print(f"Insert skipped for patient {patient_name}, {patient_dob} as it's already present in db.")
            connection.commit()
            print("Patient data inserted successfully")
        except Exception as e:
            connection.rollback()
            print("Transaction rolled back due to error:", str(e))
        finally:
            connection.autocommit = True
            cursor.close()
            connection.close()

    def InsertProviderRecords(self, providerRecords):
        connection = self.ConnectDB()
        cursor = connection.cursor()
        try:
            connection.autocommit = False
            for record in providerRecords:
                provider_name = record.name
                provider_npi = record.provider_npi
                if self.ReadProviderID(provider_npi) is None:
                    query = f'INSERT INTO Providers (provider_name, provider_npi) VALUES ("{provider_name}", "{provider_npi}")'
                    cursor.execute(query)
                else:
                    print(f"Insert skipped for provider {provider_name}, {provider_npi} as it's already present in db.")
            connection.commit()
            print("Provider data inserted successfully")
        except Exception as e:
            connection.rollback()
            print("Transaction rolled back due to error:", str(e))
        finally:
            connection.autocommit = True
            cursor.close()
            connection.close()

    def InsertEncounterRecords(self, encounterRecords):
        connection = self.ConnectDB()
        cursor = connection.cursor()
        try:
            connection.autocommit = False
            for record in encounterRecords:
                query = f'INSERT INTO Encounters (patient_id, encounter_date, provider_id, encounter_note) VALUES ({record.patient_id}, "{record.encounter_date}", {record.provider_id}, "{record.encounter_note}")'
                cursor.execute(query)
            connection.commit()
            print("Encounter data inserted successfully")
        except Exception as e:
            connection.rollback()
            print("Transaction rolled back due to error:", str(e))
        finally:
            connection.autocommit = True
            cursor.close()
            connection.close()

    def InsertChiefComplaintsRecords(self, chiefComplaintsRecords):
        connection = self.ConnectDB()
        cursor = connection.cursor()
        try:
            connection.autocommit = False
            for record in chiefComplaintsRecords:
                query = f'INSERT INTO Chief_Complaints (encounter_id, chief_complaint) VALUES ({record.encounter_id}, "{record.chief_complaint}")'
                cursor.execute(query)
            connection.commit()
            print("Chief Complaints data inserted successfully")
        except Exception as e:
            connection.rollback()
            print("Transaction rolled back due to error:", str(e))
        finally:
            connection.autocommit = True
            cursor.close()
            connection.close()

    def ReadPatientRecord(self, patientName, patientDOB):
        connection = self.ConnectDB()
        cursor = connection.cursor(buffered=True)
        patientDetails = None
        try:
            query = f'SELECT patient_name, dob, patient_id FROM patients WHERE patient_name = "{patientName}" AND dob = "{str(patientDOB)}"'
            cursor.execute(query)
            rows = cursor.fetchall()
            if(cursor.rowcount > 0):
                first_row = rows[0]
                patientDetails =  PatientDetails(first_row[0], first_row[1], first_row[2])
        except Exception as e:
            print("Error occurred while reading patient record:", str(e))
        finally:
            cursor.close()
            connection.close()
            return patientDetails

    def ReadPatientID(self, patient_name, patientDOB):
        connection = self.ConnectDB()
        cursor = connection.cursor(buffered=True)
        patient_id = None
        try:
            query = f'SELECT patient_id FROM patients WHERE patient_name = "{patient_name}" AND dob = "{str(patientDOB)}"'
            cursor.execute(query)
            if cursor.rowcount > 0:
                patient_id = cursor.fetchone()[0]
        except Exception as e:
            print("Error occurred while reading patient ID:", str(e))
        finally:
            cursor.close()
            connection.close()
            return patient_id

    def ReadProviderID(self, provider_npi):
        connection = self.ConnectDB()
        cursor = connection.cursor(buffered=True)
        provider_id = None
        try:
            query = f'SELECT provider_id FROM providers WHERE provider_npi = "{provider_npi}"'
            cursor.execute(query)
            if cursor.rowcount > 0:
                provider_id = cursor.fetchone()[0]
        except Exception as e:
            print("Error occurred while reading provider ID:", str(e))
        finally:
            cursor.close()
            connection.close()
            return provider_id

    def ReadEncounterID(self, patient_id, provider_id):
        connection = self.ConnectDB()
        cursor = connection.cursor(buffered=True)
        encounter_id = None
        try:
            query = f'SELECT encounter_id FROM Encounters WHERE patient_id = {patient_id} AND provider_id = {provider_id}'
            cursor.execute(query)
            if cursor.rowcount > 0:
                encounter_id = cursor.fetchone()[0]
        except Exception as e:
            print("Error occurred while reading encounter ID:", str(e))
        finally:
            cursor.close()
            connection.close()
            return encounter_id
class PatientDetails:
    def __init__(self, name, dob,id = 0):
        self.id = id
        self.name = name
        self.dob = dob

class ProviderDetails:
    def __init__(self, name, provider_npi, id =0):
        self.id = id
        self.name = name
        self.provider_npi = provider_npi

class EncounterDetails:
    def __init__(self, patient_id, encounter_date, provider_id, encounter_note, id=0):
        self.id = id
        self.patient_id = patient_id
        self.encounter_date = encounter_date
        self.provider_id = provider_id
        self.encounter_note = encounter_note

class ChiefComplaint:
    def __init__(self, encounter_id, chief_complaint, id = 0):
        self.id = id
        self.encounter_id = encounter_id
        self.chief_complaint = chief_complaint

class FileRecord:
    def __init__(self, patient_name, dob, encounter_date, provider, encounter_note, chief_complaint, provider_npi):
        self.patient_name = patient_name
        self.dob = dob
        self.encounter_date = encounter_date
        self.provider = provider
        self.encounter_note = encounter_note
        self.chief_complaint = chief_complaint
        self.provider_npi = provider_npi


date_formats = [
    "%B %d, %Y",
    "%d-%b-%y",
    "%d-%m-%Y",
    "%m/%d/%Y"
]

def format_date(date_str):
    for format in date_formats:
        try:
            return datetime.strptime(date_str, format).date()
        except Exception:
            pass
    print(f"format not found for date {date_str}")


def ExtractCorrectMonth(month):
    correct_month_names = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October',
                   'November', 'December']
    splitted_month = month.split()
    if(splitted_month[0].isalpha()):
        splitted_month[0] = get_close_matches(splitted_month[0], correct_month_names, n=1)[0]
        return " ".join(splitted_month)
    return month

def ExtractCorrectComplaints(cheif_complaint):
    correct_complaints_names = ['diabetes', 'bruising', 'insomnia', 'headache', 'cough', 'high blood pressure', 'heart disease',
                        'hyper tension', 'indigestion', 'stomach ache']
    return get_close_matches(cheif_complaint, correct_complaints_names, n=1)[0]

def ReadCSV(fileName):
    return pd.read_csv(fileName)

def EliminateRecordsForMissingData(data):
    return data.dropna()

def ExtractRecordsFromCSV(cleanData):
    allRecords = []
    for (patient_name, dob, enconter_date, provider_name, encounter_notes, chief_complaint, provider_npi) in zip((cleanData['patient']), cleanData['dob'], cleanData['encounter date'], cleanData['provider'], cleanData['encounter note'], cleanData['chief complaint'], cleanData['provider npi']):
        record = FileRecord(patient_name,dob,enconter_date,provider_name,encounter_notes, chief_complaint, provider_npi)
        allRecords.append(record)
    return allRecords

def GetAllPatientRecords(extractedRecords):
    allPatentRecords = []
    for data in extractedRecords:
        formatted_dob = format_date(ExtractCorrectMonth(data.dob))
        patientDetails = PatientDetails(data.patient_name, formatted_dob)
        allPatentRecords.append(patientDetails)
    return allPatentRecords

def GetAllProviderRecords(extractedRecords):
    unique_provider_npis = set(record.provider_npi for record in extractedRecords)
    provider_details_array = []
    for npi in unique_provider_npis:
        record = next((record for record in extractedRecords if record.provider_npi == npi), None)
        if record:
            provider_details_array.append(ProviderDetails(record.provider, npi))
    return provider_details_array

def ExtractEncounterRecords(extractedRecords, patient_records):
    allEncounterRecords = []
    for data, patient_data in zip(extractedRecords, patient_records):
        patient_id = DBManager().ReadPatientID(patient_data.name, patient_data.dob)
        provider_id = DBManager().ReadProviderID(data.provider_npi)
        encounter_date = format_date(ExtractCorrectMonth(data.encounter_date))
        encounter_record = EncounterDetails(patient_id, encounter_date, provider_id, data.encounter_note)
        allEncounterRecords.append(encounter_record)
    return allEncounterRecords

def ExtractChiefComplaintsRecords(extractedRecords, patient_records):
    allChiefComplaintsRecords = []
    for data, patient_data in zip(extractedRecords, patient_records):
        patient_id = DBManager().ReadPatientID(patient_data.name, patient_data.dob)
        provider_id = DBManager().ReadProviderID(data.provider_npi)
        encounter_id = DBManager().ReadEncounterID(patient_id,provider_id)
        chief_complaint_record = ChiefComplaint(encounter_id, data.chief_complaint)
        allChiefComplaintsRecords.append(chief_complaint_record)
    return allChiefComplaintsRecords


if(__name__ == "__main__"):
    data = ReadCSV("File.csv")
    cleaned_data = EliminateRecordsForMissingData(data)
    all_data = ExtractRecordsFromCSV(cleaned_data)
    patient_records = GetAllPatientRecords(all_data)
    DBManager().InsertPatientRecords(patient_records)
    providers_records = GetAllProviderRecords(all_data)
    DBManager().InsertProviderRecords(providers_records)
    encounter_records = ExtractEncounterRecords(all_data, patient_records)
    DBManager().InsertEncounterRecords(encounter_records)
    chief_complaint_records = ExtractChiefComplaintsRecords(all_data, patient_records)
    DBManager().InsertChiefComplaintsRecords(chief_complaint_records)
    print("Data load completed successfully")



