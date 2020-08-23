#pragma once

extern "C" __declspec (dllexport) int __cdecl ExecuteQuery(const char * Server, const char * Port, const char * Schema, const char * Username, const char * Password, const char * Query, const char * LogFile);