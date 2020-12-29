# CRD
#include "JSONHandler.h"
#include "AccessFacade.h"
#include <fstream>
#include <thread>
using namespace std;
DataHandler *DataHandler::instance = 0;
void DataHandler::lockMutex()
{
  dataCriticalMutex.lock();
}

void DataHandler::unLockMutex()
{
  dataCriticalMutex.unlock();
}
DataHandler::DataHandler()
{
    std::thread(&DataHandler::purgeData,this);    
}

void DataHandler::purgeData()
{
    while(1)
    {
        std::this_thread::sleep_for (std::chrono::seconds(5));
        tll_mutex.lock();
        map<string,long long>::iterator itr = timeToLive.begin();
        long long milliseconds_since_epoch =
        std::chrono::system_clock::now().time_since_epoch() /
        std::chrono::milliseconds(1000);
        vector<string> toDelete;
        while(itr != timeToLive.end())
        {
            if(itr->second > milliseconds_since_epoch)
	    {
	        toDelete.push_back(itr->first);
                itr = timeToLive.erase(itr);
     	    }	
	    else
	    {
	        ++itr;
	    }
        }
        tll_mutex.unlock();
        lockMutex();
        for(int i = 0;i < toDelete.size();i++)
        {
            deleteFromMap(toDelete[i]);
        }
        unLockMutex();
        toDelete.clear();
    }
}
bool DataHandler::checkIfDataExistMap(string key)
{
    map<string,string>::iterator it = dataStorage.find(key);
    if(it == dataStorage.end())
        return false;
    return true;
}

string DataHandler::getDataFromMap(string key)
{
    map<string,string>::iterator it = dataStorage.find(key);
    if(it != dataStorage.end())
      return it->second;
    return "";
}

bool DataHandler::deleteFromMap(string key)
{
   map<string,string>::iterator it = dataStorage.find(key);
   if(it != dataStorage.end())
   {
       dataStorage.erase(it->first);
       return true;
   }
   return false;
}
bool DataHandler::insertIntoMap(string key,string value,long long time)
{
   map<string,string>::iterator it = dataStorage.find(key);
   if(it == dataStorage.end())
   {
     dataStorage.insert(pair<string,string>(key,value));
     if(time > 0)
     {
	 long long milliseconds_since_epoch =
         std::chrono::system_clock::now().time_since_epoch() /
         std::chrono::milliseconds(1000);
	 tll_mutex.lock();
         timeToLive.insert(pair<string,long long>(key,milliseconds_since_epoch+time));
	 tll_mutex.unlock();
     }
     return true;
   }
   return false;
}

bool DataHandler::insertData(std::string key,Json::Value &obj,long long timeToLive)
{
    lockMutex();
    if(checkIfDataExistMap(key))
    {
      unLockMutex();
      cout<<endl<<"Error: data already exists";
      return false;
    }
    else
    {
      unLockMutex();
      JSONHandler *jsonHnd = new JSONHandler();
      string jsonStr = jsonHnd->convertToString(obj);
      delete jsonHnd;
      jsonHnd = NULL;
      lockMutex();
      insertIntoMap(key,jsonStr);
      //writeDataToFile(key,jsonStr);
    }
    unLockMutex();
}
Json::Value DataHandler::readData(string key)
{
    lockMutex();
    string value = getDataFromMap(key);
    unLockMutex();
    JSONHandler *jsonHnd = new JSONHandler();
    Json::Value data = jsonHnd->convertToJSON(value);
    delete jsonHnd;
    jsonHnd = NULL;
    return data;
}
bool DataHandler::deleteData(string key)
{
    lockMutex();
    deleteFromMap(key);
    //deleteFromFile(key);
    unLockMutex();
}
void DataHandler::updateFilesWithMapData()
{
    lockMutex();
    if(filePath == "")
    {
        filePath = "dataStore";
    }
    for(map<string,string>::iterator it=dataStorage.begin(); it!=dataStorage.end(); ++it)
    {
        writeDataToFile(it->first,it->second);
    }
    
    unLockMutex();
}
void DataHandler::setFilePath(string data)
{
    filePath = data;
}
DataHandler * DataHandler::getInstance() 
{
  if (!instance)
  {
      instanceMutex.lock();
      if(!instance)
          instance = new DataHandler;
      instanceMutex.unlock();
  }
  return instance;
}
void DataHandler::resetInstance()
{
    updateFilesWithMapData();
    delete instance;
    instance = NULL;
}
int DataHandler::writeDataToFile(string key,string value)
{
    ofstream dataFile(filePath);
    long long time = 0;
    tll_mutex.lock();
    if(timeToLive.find(key) != timeToLive.end())
    {
        time = timeToLive[key];
    }
    tll_mutex.lock();
    dataFile<<key<<"::::"<<key<<":::::"<<value<< endl;
    dataFile.close();
}
int DataHandler::readDataFromFile()
{
    lockMutex();
    string data;
    ifstream inFile;
    inFile.open(filePath);
    while (std::getline(inFile, data))
    {
        size_t pos = data.find("::::");
	string key = data.substr(0,pos-1);
	size_t pos1 = data.find(":::::");
	string value = data.substr(pos1+5,data.length()- pos1 - 5);
        insertIntoMap(key,value);
	string time = data.substr(pos+4,pos1-1);
	long long l_time = stoll(time);
	tll_mutex.lock();
	timeToLive.insert(pair<string,long long>(key,l_time));
	tll_mutex.unlock();
    }
    inFile.close();
    unLockMutex();
}
