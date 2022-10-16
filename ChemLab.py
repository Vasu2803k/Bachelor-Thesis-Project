
# connect with the database
import sys
import sqlite3
import csv
import pandas as pd

class Molecules():
    # Initializing the connection and cursor object to it.
    def __init__(self):
        self.connection = None
        self.cursor = None

    def CreateConnection(self, database_name):
        try:
            self.connection = sqlite3.connect(database_name, check_same_thread=False)
        except Exception as e:
            print("Error: "+str(e))
        else:
            print("connection succeeded")
        return self.connection

    # Databse Management
    def Load_Molecules(self):
        # All Commands related to database;
        # Chemdatafile - CSV file
        query_drop="DROP TABLE IF EXISTS chemdatafile;"
        try:
            self.cursor.execute(query_drop)
        except Exception as e:
            print("Query failed while executing the defined statement!: "+str(e))
        else:
            print("Query successful!")

        query1 ='''
            CREATE TABLE IF NOT EXISTS chemdatafile (
            mol_no INT AUTO_INCREMENT,mol_id VARCHAR(10) NOT NULL, smiles VARCHAR(29) NOT NULL, `A` DECIMAL(38, 5) NOT NULL, `B` DECIMAL(38, 7) NOT NULL, `C` DECIMAL(38, 7) NOT NULL, mu DECIMAL(38, 4) NOT NULL, alpha DECIMAL(38, 2) NOT NULL,homo DECIMAL(38, 4) NOT NULL, lumo DECIMAL(38, 4) NOT NULL, gap DECIMAL(38, 4) NOT NULL, r2 DECIMAL(38, 4) NOT NULL, zpve DECIMAL(38, 6) NOT NULL, u0 DECIMAL(38, 6) NOT NULL, u298 DECIMAL(38, 6) NOT NULL, h298 DECIMAL(38, 6) NOT NULL, g298 DECIMAL(38, 6) NOT NULL, cv DECIMAL(38, 3) NOT NULL, u0_atom DECIMAL(38, 7) NOT NULL, u298_atom DECIMAL(38, 7) NOT NULL, 
            h298_atom DECIMAL(38, 7) NOT NULL, g298_atom DECIMAL(38, 7) NOT NULL
            );
        '''
        file = open('.\chemdatafile.csv')
        # Now, Read the contents from the file using csv reader
        contents = csv.reader(file)
        # Already the table has been created, the rest is to insert all the info
        query2 = "INSERT INTO chemdatafile values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);"

        try:
            self.cursor.execute(query1)
            self.cursor.executemany(query2, contents)
        except Exception as e:
            print("Query failed while executing the defined statement!: "+str(e))
        else:
            print("Query successful!")

    # Retrieving the Information
    def RetrieveFullInfo(self, table_name):
        RetrievedList = []
        query = f"SELECT * FROM {table_name};"
        self.cursor.execute(query)
        RetrievedList = self.cursor.fetchall()
        return RetrievedList

    
    def RetrieveElements_AND(self, table_name, element1='', element2='', element3='', element4='', element5=''):
        RetrievedList = []
        query = f"SELECT * FROM {table_name} WHERE (smiles like '%{element1}%' or smiles like '%{element1.lower()}%')  AND (smiles like '%{element2}%' or smiles like '%{element2.lower()}%') AND (smiles like '%{element3}%' or smiles like '%{element3.lower()}%') AND (smiles like '%{element4}%' or smiles like '%{element4.lower()}%') AND (smiles like '%{element5}%' or smiles like '%{element5.lower()}%');"
        try:
            self.cursor.execute(query)
            RetrievedList = self.cursor.fetchall()
        except Exception as e:
            print("Query failed while executing the defined statement!: "+str(e))
        else:
            print("Query successful!")
        return RetrievedList

    def RetrieveElements_OR(self, table_name, element1=None, element2=None, element3=None, element4=None, element5=None):
        RetrievedList = []
        query = f"SELECT * FROM {table_name} WHERE smiles like '%{element1}%' or smiles like '%{element2}%' or smiles like '%{element3}%' or smiles like '%{element4}%' or smiles like '%{element5}%';"
        try:
            self.cursor.execute(query)
            RetrievedList = self.cursor.fetchall()
        except Exception as e:
            print("Query failed while executing the defined statement!: "+str(e))
        else:
            print("Query successful!")
        return RetrievedList

    def RetrieveElements_AND1(self, table_name, element1='', element2='', element3='', element4='', element5='', A_val=None, B_val=None, C_val=None, mu_val=None, alpha_val=None, homo_val=None, lumo_val=None, gap_val=None, zpve_val=None, u0_val=None, u298_val=None, h298_val=None, g298_val=None, cv_val=None, u0_atom_val=None, u298_atom_val=None, h298_atom_val=None, g298_atom_val=None):
        RetrievedList = []
        query = f"SELECT * FROM {table_name} WHERE smiles like '%{element1}%' AND smiles like '%{element2}%' AND smiles like '%{element3}%' AND smiles like '%{element4}%' AND smiles like '%{element5}%' and A={A_val} and B={B_val} and C={C_val} and mu={mu_val} and alpha={alpha_val} and homo={homo_val} and lumo={lumo_val} and gap={gap_val} and zpve={zpve_val} and u0={u0_val} and u298={u298_val} and h298={h298_val} and g298={g298_val} and cv={cv_val} and u0_atom={u0_atom_val} and u298_atom={u298_atom_val} and h298_atom={h298_atom_val} and g298_atom={g298_atom_val};"

        try:
            self.cursor.execute(query)
            RetrievedList = self.cursor.fetchall()
        except Exception as e:
            print("Query failed while executing the defined statement!: "+str(e))
        else:
            print("Query successful!")
        return RetrievedList

    def MolFromStoichiometry(self, table_name, elementH=('', 0), elementC=('', 0), elementO=('', 0), elementF=('', 0), elementN=('', 0)):
        RetrievedList = []

        query = f"SELECT smiles FROM {table_name} WHERE smiles like '%{elementH[0]}%' AND smiles like '%{elementC[0]}%' AND smiles like '%{elementO[0]}%' AND smiles like '%{elementF[0]}%' AND smiles like '%{elementN[0]}%';"
        try:
            self.cursor.execute(query)
            RetrievedList = self.cursor.fetchall()
        except Exception as e:
            print("Query failed while executing the defined statement!: "+str(e))
        else:
            print("Query successful!")

    def MolFromSmile(self, table_name, smile_rep):
        print("the smile rep is: "+smile_rep)
        RetrievedList=[]
        query = f"SELECT * FROM {table_name} WHERE smiles=(?) or smiles=(?);"
        try:
            self.cursor.execute(query, (smile_rep, smile_rep.upper()))
            RetrievedList = self.cursor.fetchall()
            print(RetrievedList)
        except Exception as e:
            print("Query failed while executing the defined statement!: "+str(e))
        else:
            print("Query successful!")
        return RetrievedList

    def RetrieveRotationalConstants(self, table_name, a, b, c):
        query = f"SELECT * FROM {table_name} WHERE A=? AND B=? AND C=?;"
        self.cursor.execute(query, (a, b, c,))
        RetrievedList = self.cursor.fetchall()
        return RetrievedList

    def RetrieveDipoleMoment(self, table_name, DipoleMoment):
        query = f"SELECT * FROM {table_name} WHERE mu=?;"
        self.cursor.execute(query, (DipoleMoment,))
        RetrievedList = self.cursor.fetchall()
        return RetrievedList

    def RetreiveInternalConstant(self, table_name, InternalConstant):
        query = f"SELECT * FROM {table_name} WHERE alpha={InternalConstant};"
        self.cursor.execute(query)
        RetrievedList = self.cursor.fetchall()
        return RetrievedList

    def RetrieveHighestOccupiedMolecularOrbit(self, table_name, MO):
        query = f"SELECT * FROM {table_name} WHERE homo=?;"
        self.cursor.execute(query, (MO,))
        RetrievedList = self.cursor.fetchall()
        return RetrievedList

    def RetrieveLowestUnoccupiedMolecularOrbit(self, table_name, MO):
        query = f"SELECT * FROM {table_name} WHERE lumo=?;"
        self.cursor.execute(query, (MO,))
        RetrievedList = self.cursor.fetchall()
        return RetrievedList

    def RetrieveGap(self, hl, table_name):
        query = f"SELECT * FROM {table_name} WHERE GAP={hl};"
        self.cursor.execute(query)
        RetrievedList = self.cursor.fetchall()
        return RetrievedList

    def RetrieveScore(self, table_name, score):
        query = f"SELECT * FROM {table_name} WHERE r2={score};"
        self.cursor.execute(query)
        RetrievedList = self.cursor.fetchall()
        return RetrievedList

    def RetrieveZeroPointVibrationalEnergy(self, table_name, VE):
        query = f"SELECT * FROM {table_name} WHERE ZPVE={VE};"
        self.cursor.execute(query)
        RetrievedList = self.cursor.fetchall()
        return RetrievedList

    def RetrieveInternalEnergy_0(self, table_name, u0=0.0):
        query = f"SELECT * FROM {table_name} WHERE u0={u0};"
        self.cursor.execute(query)
        RetrievedList = self.cursor.fetchall()
        return RetrievedList

    def RetrieveInternalEnergy_298(self, table_name, u298=None):
        query = f"SELECT * FROM {table_name} WHERE u298={u298};"
        self.cursor.execute(query)
        RetrievedList = self.cursor.fetchall()
        return RetrievedList

    def RetrieveEnthalpy_298(self, table_name, h298=None):
        query = f"SELECT * FROM {table_name} WHERE h298={h298};"
        self.cursor.execute(query)
        RetrievedList = self.cursor.fetchall()
        return RetrievedList

    def RetrieveGibbsFreeEnergy_298(self, table_name, g298=None):
        query = f"SELECT * FROM {table_name} WHERE g298={g298};"
        self.cursor.execute(query)
        RetrievedList = self.cursor.fetchall()
        return RetrievedList

    def RetrieveMolarHeatCapacity(self, table_name, cv=None):
        query = f"SELECT * FROM {table_name} WHERE cv={cv};"
        self.cursor.execute(query)
        RetrievedList = self.cursor.fetchall()
        return RetrievedList

    def RetrieveInternalEnergy_atom_0(self, table_name, u0_atom=None):
        query = f"SELECT * FROM {table_name} WHERE u0_atom={u0_atom};"
        self.cursor.execute(query)
        RetrievedList = self.cursor.fetchall()
        return RetrievedList

    def RetrieveInternalEnergy_atom_298(self, table_name, u298_atom=None):
        query = f"SELECT * FROM {table_name} WHERE u298_atom={u298_atom};"
        self.cursor.execute(query)
        RetrievedList = self.cursor.fetchall()
        return RetrievedList

    def RetrieveEnthalpy_atom_298(self, table_name, h298_atom=None):
        query = f"SELECT * FROM {table_name} WHERE h298_atom={h298_atom};"
        self.cursor.execute(query)
        RetrievedList = self.cursor.fetchall()
        return RetrievedList

    def RetrieveGibbsFreeEnergy_atom_298(self, table_name, g298_atom=None):
        query = f"SELECT * FROM {table_name} WHERE g298_atom={g298_atom};"
        self.cursor.execute(query)
        RetrievedList = self.cursor.fetchall()
        return RetrievedList

    def hydrocarbons_utility(self,table_name):
        
        columns = ["smiles", "mol_no"]
        #rows1 = self.RetrieveElements_AND("pubchemfile", element1='C', element2='O')
        rows = self.RetrieveElements_AND(table_name, element1='C', element2='N')
        #rows3 = self.RetrieveElements_AND("pubchemfile", element1='C', element2='F')
        #rows = self.RetrieveFullInfo('pubchemfile')
        print("Retrieving has been done!")
        for ls in rows:
            if(not(all(i=='C' or i=='N' or i in '%[^0-9]%' for i in ls))):
                rows.remove(ls)
        
        print("removal has been done!")
        with open("HydroCarbons_smiles",'w') as out_file:
            csv_writer=csv.writer(out_file)
            csv_writer.writerow(columns)
            csv_writer.writerows(rows)
        return
        
        """
        RetrievedList = []
        query = f"SELECT * FROM {table_name} WHERE (smiles like '%{element1}%' or smiles like '%{element1.lower()}%') AND (smiles like '%{element2}%' or smiles like '%{element2.lower()}%')  AND (smiles not like '%{element3}%' or smiles not like '%{element3.lower()}%') AND (smiles not like '%{element4}%' or smiles not like '%{element4.lower()}%') AND (smiles not like '%{element5}%' or smiles not like '%{element5.lower()}%');"

        try:
            self.cursor.execute(query)
            RetrievedList = self.cursor.fetchall()
        except Exception as e:
            print("Query failed while executing the defined statement!: "+str(e))
        else:
            print("Query successful!")
        columns = ["smiles", "mol_no"]
        with open("HydroCarbons_smiles",'w') as out_file:
            csv_writer=csv.writer(out_file)
            csv_writer.writerow(columns)
            csv_writer.writerows(RetrievedList)
        
        return
        """
    def to_csv_utility(self):

        # columns for chemdatafile
        '''columns = ["mol_no", "mol_id", "smiles", 'A', 'B', 'C', "mu", "alpha", "homo", "lumo", "gap", "r2",
                   "zpve", "u0", "u298", "h298", "g298", "cv", "u0_atom", "u298_atom", "h298_atom", "g298_atom"]'''
        # columns for pubchemfile
        columns = ["smiles", "mol_no"]
        rows = self.RetrieveElements_AND(
            "pubchemfile", element1='C', element2='O')
        # To CSV File from two lists with rows and columns
        with open("Oxy-Carbon_smiles", 'w') as file:
            write_file = csv.writer(file)
            write_file.writerow(columns)
            write_file.writerows(rows)
        return

    def dat_to_excel(self):
        # Limitation of rows where a sheet can have only 1048576 rows ->csv is the option
        df = pd.read_table(
            "D:\Projects\pubchem_data\pubchem.tar\pubchem\pubchem.dat")
        # df.to_excel('pubchem.xlsx')
        # print(df.head())
        return

    def dat_to_csv(self):
        columns = ["smiles", "serial No."]
        #in_file=open("filepath", 'r')
        #out_file=open("file", 'w')
        with open("D:\Projects\pubchem_data\pubchem.tar\pubchem\pubchem.dat", 'r') as in_file:
            with open("pubchem.csv", 'w') as out_file:
                csv_writer = csv.writer(out_file)
                csv_writer.writerow(columns)
                for row in in_file:
                    row_values = [row_val.strip()
                                  for row_val in row.split(',')]
                    csv_writer.writerow(row_values)
        return

if (__name__ == "__main__"):
    print("Main file is being used!")
    QueryDB = Molecules()
    # Connect with the sqlite databse
    QueryDB.CreateConnection("chemdatabase.db")
    QueryDB.cursor = QueryDB.connection.cursor()
    #QueryDB.hydrocarbons_utility('pubchemfile')
    QueryDB.connection.commit()
else:
    print("Other File Usage!")
    QueryDB = Molecules()
    # Connect with the sqlite databse
    QueryDB.CreateConnection("chemdatabase.db")
    QueryDB.cursor = QueryDB.connection.cursor()
    QueryDB.Load_Molecules()
    QueryDB.connection.commit()
    #QueryDB.connection.close()
