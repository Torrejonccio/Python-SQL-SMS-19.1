import pyodbc as pbc
import zipfile
import pandas as pd


server = "DESKTOP-66CIB2B\SQLEXPRESS"
database = "Fut_usm"

'''
La función recibe el server y el nombre de la base de datos
Genera la conexión a la base y la crea en caso de no existir
Si no hay ningún error, retorna la conexión y el cursor
'''
def connect_db(server, database):
    try:
        connection = pbc.connect(f"DRIVER={{SQL Server}};SERVER={server};Trusted_Connection=yes;", autocommit=True)
        cursor = connection.cursor()
        cursor.execute(f"""
        IF NOT EXISTS(
            SELECT * FROM sys.databases WHERE name = N'{database}'
        )

        CREATE DATABASE {database}
        """)

        cursor.execute(f"USE {database}")
        cursor.commit()
        return connection, cursor

    except Exception as error:
        print("Ocurrió un error: ", error)
        if connection:
            connection.close()
        return None, None


'''
Recibe el cursor y el nombre de la tabla a verificar
Verifica si la tabla existe o no
Retorna la cantidad de apariciones de la tabla
'''
def verify_tables(cursor, table_name):
    cursor.execute(f"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{table_name}'")
    return cursor.fetchone()[0]


'''
Recibe el cursor
Si la base no contiene las tablas, las crea
'''
def create_tables(cursor):
    if verify_tables(cursor, "WorldcupsInfo") == 0:
        cursor.execute("""
            CREATE TABLE WorldcupsInfo (
            Id INT IDENTITY(1, 1) PRIMARY KEY,
            Year INT,
            Host NVARCHAR(50),
            Champion NVARCHAR(50),
            Runner_up NVARCHAR(50),
            Third_place NVARCHAR(50),
            Teams INT,
            Matches_played INT,
            Goals_scored INT,
            Avg_goals_pergame FLOAT    
            )
        """)
    if verify_tables(cursor, "TeamsInfo") == 0:
        cursor.execute("""
            CREATE TABLE TeamsInfo (
            Id INT IDENTITY(1, 1) PRIMARY KEY,
            IdWorldcup INT,
            Team NVARCHAR(50),
            Position INT,
            Games_played INT,
            Win INT,
            Draw INT,
            Loss INT,
            Goals_for INT,
            Goals_against INT,
            Goal_difference INT,
            Points INT,
            CONSTRAINT FK_TeamsInfo_WorldcupsInfo FOREIGN KEY (IdWorldcup) REFERENCES WorldcupsInfo(Id)
            )
        """)

    cursor.commit()


'''
Recibe la conexión
Cierra la conexión con la base
'''
def close_connection(connection):
    cursor.commit()
    connection.close()


'''
Recibe el cursor
Inserta la data de los CSV en las tablas correspondientes
'''
def insert_data(cursor):
    dicc = {
        "1930":1, 
        "1934":2, 
        "1938":3, 
        "1950":4, 
        "1954":5, 
        "1958":6, 
        "1962":7,
        "1966":8, 
        "1970":9, 
        "1974":10, 
        "1978":11, 
        "1982":12, 
        "1986":13, 
        "1990":14, 
        "1994":15, 
        "1998":16, 
        "2002":17,
        "2006":18,
        "2010":19,
        "2014":20,
        "2018":21,
        "2022":22
        }
    

    with zipfile.ZipFile("FIFA.zip", "r") as zip:
        file_namelist = zip.namelist() # Obtiene una lista de los csv
        with open(f"temp_directory/FIFA - World Cup Summary.csv", "r") as fifa_file:
            fifa_df = pd.read_csv(fifa_file)
            for row_index, row in fifa_df.iterrows():
                cursor.execute(f"""
                    INSERT INTO WorldcupsInfo (
                        Year,
                        Host,
                        Champion,
                        Runner_up,
                        Third_place,
                        Teams,
                        Matches_played,
                        Goals_scored,
                        Avg_goals_pergame                                              
                    )
                    VALUES (
                        '{row["YEAR"]}',
                        '{row["HOST"]}',
                        '{row["CHAMPION"]}',
                        '{row["RUNNER UP"]}',
                        '{row["THIRD PLACE"]}',
                        '{row["TEAMS"]}',
                        '{row["MATCHES PLAYED"]}',
                        '{row["GOALS SCORED"]}',
                        '{row["AVG GOALS PER GAME"]}'
                    )

                """)

        for file_name in file_namelist:
            with open(f"temp_directory/{file_name}", "r", encoding="utf-8") as file:
                df = pd.read_csv(file)
                if "YEAR" not in df.columns:                    
                    IdWorldcup = dicc[file_name[-8:-4]]
                    for row_index, row in df.iterrows():
                        cursor.execute(f"""
                            INSERT INTO TeamsInfo (
                                IdWorldcup,
                                Team,
                                Position,
                                Games_played,
                                Win,
                                Draw,
                                Loss,
                                Goals_for,
                                Goals_against,
                                Goal_difference,
                                Points                                              
                            )
                            VALUES (
                                '{IdWorldcup}',
                                '{row["Team"]}',
                                '{row["Position"]}',
                                '{row["Games Played"]}',
                                '{row["Win"]}',
                                '{row["Draw"]}',
                                '{row["Loss"]}',
                                '{row["Goals For"]}',
                                '{row["Goals Against"]}',
                                '{row["Goal Difference"]}',
                                '{row["Points"]}'                                                              
                            )

                        """)   
    cursor.commit()                                                             
    return


'''
Recibe el cursor
Muestra todos los países que han sido campeones junto al año respectivo
'''
def show_champs(cursor):
    cursor.execute("""
    SELECT Champion, Year 
    FROM WorldcupsInfo
    """)

    results = cursor.fetchall()
    print("Los países que han sido campeones:\n")
    for line in results:
        print(f"{line.Year}: {line.Champion}\n")


'''
Recibe el cursor
Muestra los 5 países que mayor cantidad de goles han
hecho en todos los mundiales junto al número de goles totales
'''
def show_greatest_scorers(cursor):
    cursor.execute("""
    SELECT TOP 5 Team, SUM(Goals_for) AS Total_goals 
    FROM TeamsInfo 
    GROUP BY Team
    ORDER BY Total_goals DESC 
    """)

    results = cursor.fetchall()
    print("Los 5 países con mayor cantidad de goles son:\n")
    count = 0
    for line in results:
        count += 1
        print(f"{count}. {line.Team} - {line.Total_goals} Goles")


'''
Recibe el cursor
Muestra los 5 países que mayor cantidad de veces han obtenido
el tercer lugar junto a la cantidad de veces
'''
def show_most_times_third(cursor):
    cursor.execute("""
    SELECT TOP 5 Third_place, COUNT(Third_place) AS Total_third
    FROM WorldcupsInfo
    GROUP BY Third_place
    ORDER BY Total_third DESC
    """)

    results = cursor.fetchall()
    print("Los 5 países que mayor cantidad de veces han obtenido tercer puesto son:\n")
    count = 0
    for line in results:
        count += 1
        print(f"{count}. {line.Third_place} - {line.Total_third} Veces")


'''
Recibe el cursor
Muestra el país que más goles ha recibido en contra
y el número de goles
'''
def show_most_goals_against(cursor):
    cursor.execute("""
    SELECT TOP 1 Team, SUM(Goals_against) AS Total_against
    FROM TeamsInfo
    GROUP BY Team
    ORDER BY Total_against DESC
    """)

    results = cursor.fetchall()
    print("El país que más goles en contra ha recibido es:\n")
    for line in results:
        print(f"{line.Team} - {line.Total_against} Goles")


'''
Recibe el cursor
Buscar un país y toda la información relacionada a este
'''
def consult_team(cursor, team):
    cursor.execute(f"""
    SELECT t.Team, t.Position, t.Games_played, t.Win, t.Draw, t.Loss, t.Goals_for, t.Goals_against, t.Goal_difference, t.Points, w.Year
    FROM TeamsInfo t
    LEFT JOIN WorldcupsInfo w ON t.IdWorldcup = w.Id
    WHERE t.Team = '{team}'
    """)

    results = cursor.fetchall()
    print(f"Información de {team} en los mundiales:\n")
    print("|  Año  |  Posición  |  Partidos  |  Partidos  |  Partidos  |  Partidos  |  Goles a |  Goles en  |  Diferencia  |  Puntos  |")
    print("|       |            |  jugados   |  ganados   |  perdidos  |  empatados |  favor   |  contra    |   de goles   |          |")
    for line in results:
        print(f"| {line.Year}  |      {line.Position}     |      {line.Games_played}     |       {line.Win}     |      {line.Loss}     |      {line.Draw}     |       {line.Goals_for}   |     {line.Goals_against}    |     {line.Goal_difference}     |          {line.Points}  |\n")
    # Lo intenté hacer bonito y salió mal, losiemto rip


'''
Recibe el cursor
Muestra los tres países que más mundiales han jugado
y los años en los cuales participaron
'''
def show_most_worldcups_played(cursor): 
    cursor.execute("""
    SELECT TOP 3 t.Team, COUNT(t.IdWorldcup) AS Wc_played, STRING_AGG(CONVERT(NVARCHAR(100), w.Year), ', ') AS IdWorldcups
    FROM TeamsInfo t
    JOIN WorldcupsInfo w ON t.IdWorldcup = w.Id
    GROUP BY Team
    ORDER BY Wc_played DESC
    """)

    results = cursor.fetchall()
    print("El top 3 países que más han jugado en el mundial son:\n")
    count = 0
    for line in results:
        count += 1
        print(f"{count}. {line.Team} - {line.IdWorldcups}")


'''
Recibe el cursor
Muestra el país que históricamente tiene la mayor tasa de partidos
ganados en relación con los jugados
'''
def show_most_winrate(cursor):
    cursor.execute("""
    SELECT TOP 1 Team, ROUND( ( SUM(Win) / SUM(Games_played * 1.0) ) * 100 , 2) AS Rate
    FROM TeamsInfo
    GROUP BY Team
    ORDER BY Rate DESC
    """)

    results = cursor.fetchall()
    print("El país que históricamente tiene la mayor tasa de partidos ganados es:\n")
    for line in results:
        print(f"{line.Team}: {round(line.Rate, 2)}%")


'''
Recibe el cursor
Muestra los países que han ganado el mundial siendo
anfitrión de este
'''
def winning_local_teams(cursor):
    cursor.execute("""
    SELECT Host
    FROM WorldcupsInfo
    WHERE Host = Champion
    """)

    results = cursor.fetchall()
    print("Los países que han ganado el mundial siendo país anfitrión son:\n")
    for line in results:
        print(line.Host)


'''
Recibe el cursor
Muestra el país que más veces ha estado entre los
ganadores del primer, segundo o tercer lugar
'''
def most_times_on_the_podium(cursor):
    cursor.execute("""
    SELECT TOP 1 Team, SUM(CASE WHEN Position IN (1, 2, 3) THEN 1 ELSE 0 END) AS Total_pod
    FROM TeamsInfo
    GROUP BY Team
    ORDER BY Total_pod DESC    
    """)

    results = cursor.fetchall()
    print("El país que más veces ha estado en el podio es:\n")
    for line in results:
        print(line.Team)


'''
Recibe el cursor
Muestra los dos países que más veces se han
enfrentado en una final
'''
def greatest_rivals(cursor):
    cursor.execute("""
    SELECT TOP 1 Champion, Runner_up, COUNT(*) AS Riv_cont
    FROM WorldcupsInfo
    GROUP BY Champion, Runner_up
    ORDER BY COUNT(*) DESC
    """)

    results = cursor.fetchall()
    print("Los dos países que más veces han peleado el primer y segundo lugar son:\n")
    for line in results:
        print(f"{line.Champion} vs {line.Runner_up}")

# Se hace uso de las funciones de conexión y creación de tablas
connection, cursor = connect_db(server, database)
if connection and cursor:
    create_tables(cursor)

# Verifica que las tablas no tienen datos
cursor.execute("SELECT COUNT(*) FROM WorldcupsInfo")
row_num_1 = cursor.fetchone()[0]

cursor.execute("SELECT COUNT(*) FROM TeamsInfo")
row_num_2 = cursor.fetchone()[0]

# Si no tienen datos, se insertan
if row_num_1 == 0 and row_num_2 == 0:
    insert_data(cursor)

# Printea las operaciones disponibles
print("Operaciones disponibles:\n")
print("1. Mostrar campeones\n2. Mostrar top 5 goleadores\n3. Mostrar top 5 países con más veces el tercer lugar\n4. Mostrar país con más goles recibidos\n5. Buscar información de un país")
print("6. Mostrar top 3 países con más apariciones en un mundial\n7. Mostrar país con mayor tasa de partidos ganados\n8. Mostrar países que han ganado un mundial siendo país anfitrión")
print("9. Mostrar país que más veces ha estado en el podio\n10. Mostrar mayores rivales\n")

cont = 1
valid = 1
while cont:
    op = input("Seleccione una operación:\n")

    if op == "1":
        show_champs(cursor)
        while valid == 1:
            another_op = input("\n¿Desea realizar otra operación? (Y/N): ")
            if another_op == "Y":
                break
            elif another_op == "N":
                exit()
            else:
                print("ENTRADA NO VÁLIDA\n")

    elif op == "2":
        show_greatest_scorers(cursor)
        while valid == 1:
            another_op = input("\n¿Desea realizar otra operación? (Y/N): ")
            if another_op == "Y":
                break
            elif another_op == "N":
                exit()
            else:
                print("ENTRADA NO VÁLIDA\n")

    elif op == "3":
        show_most_times_third(cursor)
        while valid == 1:
            another_op = input("\n¿Desea realizar otra operación? (Y/N): ")
            if another_op == "Y":
                break
            elif another_op == "N":
                exit()
            else:
                print("ENTRADA NO VÁLIDA\n")

    elif op == "4":
        show_most_goals_against(cursor)
        while valid == 1:
            another_op = input("\n¿Desea realizar otra operación? (Y/N): ")
            if another_op == "Y":
                break
            elif another_op == "N":
                exit()
            else:
                print("ENTRADA NO VÁLIDA\n")

    elif op == "5":
        consult_team(cursor, input("Escriba el país: "))
        while valid == 1:
            another_op = input("\n¿Desea realizar otra operación? (Y/N): ")
            if another_op == "Y":
                break
            elif another_op == "N":
                exit()
            else:
                print("ENTRADA NO VÁLIDA\n")

    elif op == "6":
        show_most_worldcups_played(cursor)
        while valid == 1:
            another_op = input("\n¿Desea realizar otra operación? (Y/N): ")
            if another_op == "Y":
                break
            elif another_op == "N":
                exit()
            else:
                print("ENTRADA NO VÁLIDA\n")

    elif op == "7":
        show_most_winrate(cursor)
        while valid == 1:
            another_op = input("\n¿Desea realizar otra operación? (Y/N): ")
            if another_op == "Y":
                break
            elif another_op == "N":
                exit()
            else:
                print("ENTRADA NO VÁLIDA\n")

    elif op == "8":
        winning_local_teams(cursor)
        while valid == 1:
            another_op = input("\n¿Desea realizar otra operación? (Y/N): ")
            if another_op == "Y":
                break
            elif another_op == "N":
                exit()
            else:
                print("ENTRADA NO VÁLIDA\n")

    elif op == "9":
        most_times_on_the_podium(cursor)
        while valid == 1:
            another_op = input("\n¿Desea realizar otra operación? (Y/N): ")
            if another_op == "Y":
                break
            elif another_op == "N":
                exit()
            else:
                print("ENTRADA NO VÁLIDA\n")

    elif op == "10":
        greatest_rivals(cursor)
        while valid == 1:
            another_op = input("\n¿Desea realizar otra operación? (Y/N): ")
            if another_op == "Y":
                break
            elif another_op == "N":
                exit()
            else:
                print("ENTRADA NO VÁLIDA\n")

    else:
        print("OPERACION NO VÁLIDA\n")

close_connection(connection)

