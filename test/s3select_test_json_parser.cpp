#include <iostream>
#include "rapidjson/reader.h"
#include "rapidjson/writer.h"
#include "rapidjson/filereadstream.h"
#include "rapidjson/filewritestream.h"
#include "rapidjson/error/en.h"
#include "rapidjson/document.h"
#include "rapidjson/pointer.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/memorystream.h"

#include "s3select_json_parser.h"
#include <gtest/gtest.h>
#include <cassert>
#include <sstream>
#include <fstream>
#include <vector>
#include <unordered_map>
#include <filesystem>
#include <iostream>

class dom_traverse_v2
{
	public:
		std::stringstream ss;
		void print(const rapidjson::Value &v, std::string);
		void traverse(rapidjson::Document &d);
		void traverse_object(const rapidjson::Value &v,std::string path);
		void traverse_array(const rapidjson::Value &v,std::string path);
};

void dom_traverse_v2::print(const rapidjson::Value &v, std::string key_name)
{
	ss << key_name << " : ";

	if(v.IsString())
	{
		ss << v.GetString() << std::endl;
	}
	else
		if(v.IsInt())
		{
			ss << v.GetInt() << std::endl;
		}
		else
			if(v.IsBool())
			{
				ss << (v.GetBool() ? "true" : "false" ) << std::endl;
			}
			else
				if(v.IsNull())
				{
					ss << "null" << std::endl;
				}
				else
					if(v.IsDouble())
					{
						ss << v.GetDouble() << std::endl;
					}
					else
					{
						ss << "value not exist" << std::endl;
					}
}

void dom_traverse_v2::traverse(rapidjson::Document &d)
{
	std::string path="";

	for (rapidjson::Value::ConstMemberIterator itr = d.MemberBegin(); itr != d.MemberEnd(); ++itr)
	{
		const rapidjson::Value &v = itr->value;

		if(v.IsArray())
		{
			std::string path="";
			path.append( itr->name.GetString() );
			path.append( "/" );

			traverse_array(v, path);
		}
		else if (v.IsObject())
		{
			std::string path="";
			path.append( itr->name.GetString() );
			path.append( "/" );

			traverse_object(v, path);
		}
		else
		{
			std::string tmp = path;
			path.append( itr->name.GetString() );
			path.append( "/" );
			print(v, path);
			path = tmp;
		}

	}
}

void dom_traverse_v2::traverse_array(const rapidjson::Value &v,std::string path)
{
	std::string object_key = path;

	for (rapidjson::Value::ConstValueIterator itr = v.Begin(); itr != v.End(); ++itr)
	{
		const rapidjson::Value& array_item = *itr;
		if(array_item.IsArray())
		{
			traverse_array(array_item,object_key);
		}
		else if(array_item.IsObject())
		{
			traverse_object(array_item,object_key);
		}
		else
		{
			print(array_item, object_key);
		}
	}
}

void dom_traverse_v2::traverse_object(const rapidjson::Value &v,std::string path)
{
	std::string object_key = path;

	for (rapidjson::Value::ConstMemberIterator itr = v.MemberBegin(); itr != v.MemberEnd(); ++itr)
	{
		const rapidjson::Value& v_itr = itr->value;
		if (itr->value.IsObject())
		{
			std::string tmp = object_key;
			object_key.append( itr->name.GetString() );
			object_key.append("/");
			traverse_object(v_itr,object_key);
			object_key = tmp;
		}
		else
			if (itr->value.IsArray())
			{
				object_key.append( itr->name.GetString() );
				object_key.append("/");
				traverse_array(v_itr,object_key);
			}
			else
			{
				std::string tmp = object_key;
				object_key.append( itr->name.GetString() );
				object_key.append("/");
				print(v_itr, object_key);
				object_key = tmp;
			}
	}
}


std::string parse_json_dom(const char* file_name)
{//purpose: for testing only. dom vs sax.

	std::string final_result;
	const char* dom_input_file_name = file_name;
	std::fstream dom_input_file(dom_input_file_name, std::ios::in | std::ios::binary);
	dom_input_file.seekg(0, std::ios::end);

	// get file size
	auto sz = dom_input_file.tellg();
	// place the position at the begining
	dom_input_file.seekg(0, std::ios::beg);
	//read whole file content into allocated buffer
	std::string file_content(sz, '\0');
	dom_input_file.read((char*)file_content.data(),sz);

	rapidjson::Document document;
	document.Parse(file_content.data());

	if (document.HasParseError()) {
		std::cout<<"parsing error"<< std::endl;
		return "parsing error";
	}

	if (!document.IsObject())
	{
		std::cout << " input is not an object " << std::endl;
		return "object error";
	}

	dom_traverse_v2 td2;
	td2.traverse( document );
	final_result = (td2.ss).str();
	return final_result;
}


int RGW_send_data(const char* object_name, std::string & result)
{//purpose: simulate RGW streaming an object into s3select

	std::ifstream input_file_stream;
	MyHandler handler;
	size_t buff_sz{1024*1024*4};
	char* buff = (char*)malloc(buff_sz);

	try {
		input_file_stream = std::ifstream(object_name, std::ios::in | std::ios::binary);
	}
	catch( ... ){
		std::cout << "failed to open file " << std::endl;  
		exit(-1);
	}

	//read first chunk;
	auto read_size = input_file_stream.readsome(buff, buff_sz);
	while(read_size)
	{
		//the handler is processing any buffer size
		int status = handler.process_rgw_buffer(buff, read_size);
		if(status<0) return -1;

		//read next chunk
		read_size = input_file_stream.readsome(buff, buff_sz);
	}
	handler.process_rgw_buffer(0, 0, true);

	result = handler.get_full_result();
	return 0;
}

int test_compare(int argc, char* argv[])
{
	std::string res;
	std::ofstream o1,o2;

	RGW_send_data(argv[1],res);
	std::string res2 = parse_json_dom(argv[1]);
	o1.open(std::string(argv[1]).append(".sax.out"));
	o2.open(std::string(argv[1]).append(".dom.out"));

	o1 << res;
	o2 << res2;

	o1.close();
	o2.close();

	return 0;
}

int main(int argc,char **argv)
{
	std::string res;
	RGW_send_data(argv[1],res);

	//std::cout << res; 
}

