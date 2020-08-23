#include "stdafx.h"
#include <iostream>
#include "TheHeader.h"
#include <Windows.h>
#include "C:\ExtLib\MySQL-Connector\include\jdbc\mysql_connection.h"
#include "C:\ExtLib\MySQL-Connector\include\jdbc\cppconn\driver.h"
#include "C:\ExtLib\MySQL-Connector\include\jdbc\cppconn\exception.h"
#include "C:\ExtLib\MySQL-Connector\include\jdbc\cppconn\resultset.h"
#include "C:\ExtLib\MySQL-Connector\include\jdbc\cppconn\statement.h"
#include <chrono>
#include <fstream>

//Error Codes
//-1 = Failed to Connect to Server
//-2 = Failed to Set Schema
//-3 = Failed to Execute Query

//Execute Query
int ExecuteQuery(const char * Server, const char * Port, const char * Schema, const char * Username, const char * Password, const char * Query, const char * LogFile)
{
	try
	{
		// -Start Time Stamp-
		auto StartTime = std::chrono::steady_clock::now();

		//Log File
		remove(LogFile);
		std::ofstream Log;
		Log.open(LogFile, std::fstream::in | std::fstream::out | std::fstream::app);

		//Variables
		sql::Driver * SQLDriver;
		sql::Connection * SQLConnection;
		sql::Statement * SQLStatement;
		sql::ResultSet * SQLRes;

		std::string ServerStr = Server;
		std::string PortStr = Port;
		std::string ConnectionParams = "tcp://" + ServerStr + ":" + PortStr;

		std::cout << "Connection Parameters: " + ConnectionParams << std::endl;

		try
		{
			/* Console Output */ std::cout << "Connecting Server..." << std::endl;

			//Server Connection
			SQLDriver = get_driver_instance();
			SQLConnection = SQLDriver->connect(ConnectionParams, Username, Password);
		}

		catch (sql::SQLException &Error)
		{

			/* Log Output */
			Log << "[ERROR] Server Connection Error! (Address: " << Server << ")" << std::endl;
			Log << "[ERROR] Error Code: " << Error.getErrorCode() << std::endl;
			Log << "[ERROR] Description: " << Error.what() << std::endl;
			Log << "[ERROR] SQL State Code: " << Error.getSQLState() << std::endl;


			/* Console Output */
			std::cout << "[ERROR] Server Connection Error! (Address: " << Server << ")" << std::endl;
			std::cout << "[ERROR] Error Code: " << Error.getErrorCode() << std::endl;
			std::cout << "[ERROR] Description: " << Error.what() << std::endl;
			std::cout << "[ERROR] SQL State Code: " << Error.getSQLState() << std::endl;

			return -1;
		}

		try
		{
			/* Console Output */ std::cout << "Setting Schema..." << std::endl;

			//Set Schema (Database)
			SQLConnection->setSchema(Schema);
		}

		catch (sql::SQLException &Error)
		{
			/* Log Output */
			Log << "[ERROR] Schema Selection Error! (Schema: " << Schema << ")" << std::endl;
			Log << "[ERROR] Error Code: " << Error.getErrorCode() << std::endl;
			Log << "[ERROR] Description: " << Error.what() << std::endl;
			Log << "[ERROR] SQL State Code: " << Error.getSQLState() << std::endl;

			/* Console Output */
			std::cout << "[ERROR] Schema Selection Error! (Schema: " << Schema << ")" << std::endl;
			std::cout << "[ERROR] Error Code: " << Error.getErrorCode() << std::endl;
			std::cout << "[ERROR] Description: " << Error.what() << std::endl;
			std::cout << "[ERROR] SQL State Code: " << Error.getSQLState() << std::endl;

			return -2;
		}

		try
		{
			/* Console Output */ std::cout << "Executing Query..." << std::endl;

			//Execute Query
			SQLStatement = SQLConnection->createStatement();
			SQLRes = SQLStatement->executeQuery(Query);

			int Row = 1;

			//Display Response (Iterate Through Cells)
			int RowCounter = 1;

			while (SQLRes->next())
			{
				int ColumnCounter = 1;

				while (true)
				{
					try
					{
						/* Console Information */
						std::cout << "[R" << RowCounter << ",C" << ColumnCounter << "] " << SQLRes->getString(ColumnCounter) << std::endl;

						/* Log Output */
						Log << SQLRes->getString(ColumnCounter) << ",";
					}

					catch (const std::exception& e) 
					{ 
						Log << '\n';

						RowCounter++;  /* Rows Increment */

						break; 
					}

					ColumnCounter++; /* Columns Increment */
				}
			}
		}

		catch (sql::SQLException &Error)
		{
			/* Log Output */
			Log << "[ERROR] Query Execution Error! (Query: " << Query << ")" << std::endl;
			Log << "Error Code: " << Error.getErrorCode() << std::endl;
			Log << "Description: " << Error.what() << std::endl;
			Log << "SQL State Code: " << Error.getSQLState() << std::endl;


			/* Console Output */
			std::cout << "[ERROR] Query Execution Error!" << std::endl;
			std::cout << "Error Code: " << Error.getErrorCode() << std::endl;
			std::cout << "Description: " << Error.what() << std::endl;
			std::cout << "SQL State Code: " << Error.getSQLState() << std::endl;

			return -3;
		}

		// -End Time Stamp-
		auto EndTime = std::chrono::steady_clock::now();

		//Duration Calculation
		int Duration = std::chrono::duration_cast<std::chrono::milliseconds>(EndTime - StartTime).count();

		/* Console Information */
		std::cout << "Exectue Time: " << Duration << " ms" << std::endl;

		Log.close();

		//Clean Up
		delete SQLRes;
		delete SQLStatement;
		delete SQLConnection;

		return 0;
	}

	catch (std::exception &Error) { return -1; }
}