#include "s3select_json_parser.h"
#include <gtest/gtest.h>
#include <cassert>
#include <sstream>
#include <fstream>
#include <vector>
#include <filesystem>
#include <iostream>


// ===== base64 encode/decode

typedef unsigned char uchar;
static const std::string b = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";//=
static std::string base64_encode(const std::string &in) {
    std::string out;

    int val=0, valb=-6;
    for (uchar c : in) {
        val = (val<<8) + c;
        valb += 8;
        while (valb>=0) {
            out.push_back(b[(val>>valb)&0x3F]);
            valb-=6;
        }
    }
    if (valb>-6) out.push_back(b[((val<<8)>>(valb+8))&0x3F]);
    while (out.size()%4) out.push_back('=');
    return out;
}


static std::string base64_decode(const std::string &in) {

    std::string out;

    std::vector<int> T(256,-1);
    for (int i=0; i<64; i++) T[b[i]] = i;

    int val=0, valb=-8;
    for (uchar c : in) {
        if (T[c] == -1) break;
        val = (val<<6) + T[c];
        valb += 6;
        if (valb>=0) {
            out.push_back(char((val>>valb)&0xFF));
            valb-=8;
        }
    }
    return out;
}

//=============================================

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
	JsonParserHandler handler;
	size_t buff_sz{1024*1024*4};
	char* buff = (char*)malloc(buff_sz);
	std::function<int(std::pair < std::string, Valuesax>)> fp;

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
		int status = handler.process_json_buffer(buff, read_size);
		if(status<0) return -1;

		//read next chunk
		read_size = input_file_stream.readsome(buff, buff_sz);
	}
	handler.process_json_buffer(0, 0, true);

	free(buff);
	//result = handler.get_full_result();
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


#define TEST2 \
"ewoicm93IiA6IFsKCXsKCQkiY29sb3IiOiAicmVkIiwKCQkidmFsdWUiOiAiI2YwMCIKCX0sCgl7\
CgkJImNvbG9yIjogImdyZWVuIiwKCQkidmFsdWUiOiAiIzBmMCIKCX0sCgl7CgkJImNvbG9yIjog\
ImJsdWUiLAoJCSJ2YWx1ZSI6ICIjMDBmIgoJfSwKCXsKCQkiY29sb3IiOiAiY3lhbiIsCgkJInZh\
bHVlIjogIiMwZmYiCgl9LAoJewoJCSJjb2xvciI6ICJtYWdlbnRhIiwKCQkidmFsdWUiOiAiI2Yw\
ZiIKCX0sCgl7CgkJImNvbG9yIjogInllbGxvdyIsCgkJInZhbHVlIjogIiNmZjAiCgl9LAoJewoJ\
CSJjb2xvciI6ICJibGFjayIsCgkJInZhbHVlIjogIiMwMDAiCgl9Cl0KfQo="

#define TEST3 \
"ewogICJoZWxsbyI6ICJ3b3JsZCIsCiAgICAidCI6ICJ0cnVlIiAsCiAgICAiZiI6ICJmYWxzZSIs\
CiAgICAibiI6ICJudWxsIiwKICAgICJpIjogMTIzLAogICAgInBpIjogMy4xNDE2LAoKICAgICJu\
ZXN0ZWRfb2JqIiA6IHsKICAgICAgImhlbGxvMiI6ICJ3b3JsZCIsCiAgICAgICJ0MiI6IHRydWUs\
CiAgICAgICJuZXN0ZWQyIiA6IHsKICAgICAgICAiYzEiIDogImMxX3ZhbHVlIiAsCiAgICAgICAg\
ImFycmF5X25lc3RlZDIiOiBbMTAsIDIwLCAzMCwgNDBdCiAgICAgIH0sCiAgICAgICJuZXN0ZWQz\
IiA6ewogICAgICAgICJoZWxsbzMiOiAid29ybGQiLAogICAgICAgICJ0MiI6IHRydWUsCiAgICAg\
ICAgIm5lc3RlZDQiIDogewogICAgICAgICAgImMxIiA6ICJjMV92YWx1ZSIgLAogICAgICAgICAg\
ImFycmF5X25lc3RlZDMiOiBbMTAwLCAyMDAsIDMwMCwgNDAwXQogICAgICAgIH0KICAgICAgfQog\
ICAgfSwKICAgICJhcnJheV8xIjogWzEsIDIsIDMsIDRdCn0K"

#define TEST4 \
"ewoKICAgICJnbG9zc2FyeSI6IHsKICAgICAgICAidGl0bGUiOiAiZXhhbXBsZSBnbG9zc2FyeSIs\
CgkJIkdsb3NzRGl2IjogewogICAgICAgICAgICAidGl0bGUiOiAiUyIsCgkJCSJHbG9zc0xpc3Qi\
OiB7CiAgICAgICAgICAgICAgICAiR2xvc3NFbnRyeSI6IHsKICAgICAgICAgICAgICAgICAgICAi\
SUQiOiAiU0dNTCIsCgkJCQkJIlNvcnRBcyI6ICJTR01MIiwKCQkJCQkiR2xvc3NUZXJtIjogIlN0\
YW5kYXJkIEdlbmVyYWxpemVkIE1hcmt1cCBMYW5ndWFnZSIsCgkJCQkJIkFjcm9ueW0iOiAiU0dN\
TCIsCgkJCQkJIkFiYnJldiI6ICJJU08gODg3OToxOTg2IiwKCQkJCQkiR2xvc3NEZWYiOiB7CiAg\
ICAgICAgICAgICAgICAgICAgICAgICJwYXJhIjogIkEgbWV0YS1tYXJrdXAgbGFuZ3VhZ2UsIHVz\
ZWQgdG8gY3JlYXRlIG1hcmt1cCBsYW5ndWFnZXMgc3VjaCBhcyBEb2NCb29rLiIsCgkJCQkJCSJH\
bG9zc1NlZUFsc28iOiBbIkdNTCIsICJYTUwiXQogICAgICAgICAgICAgICAgICAgIH0sCgkJCQkJ\
Ikdsb3NzU2VlIjogIm1hcmt1cCIKICAgICAgICAgICAgICAgIH0KICAgICAgICAgICAgfQogICAg\
ICAgIH0KICAgIH0KfQoK"

#define TEST5 \
"ewoKICAgICJnbG9zc2FyeSI6IHsKICAgICAgICAidGl0bGUiOiAiZXhhbXBsZSBnbG9zc2FyeSIsCiAgICAgICAgICAgICAgICAiR2xvc3NEaXYiOiB7CiAgICAgICAgICAgICJ0aXRsZSI6ICJTIi\
wKICAgICAgICAgICAgICAgICAgICAgICAgIkdsb3NzTGlzdCI6IHsKICAgICAgICAgICAgICAgICJHbG9zc0VudHJ5IjogewogICAgICAgICAgICAgICAgICAgICJJRCI6ICJTR01MIiwKICAgICAgI\
CAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICJTb3J0QXMiOiAiU0dNTCIsCiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAiR2xvc3NUZXJtIjogIlN0YW5kYXJk\
IEdlbmVyYWxpemVkIE1hcmt1cCBMYW5ndWFnZSIsCiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAiQWNyb255bSI6ICJTR01MIiwKICAgICAgICAgICAgICAgICAgICAgIC\
AgICAgICAgICAgICAgICAgICJBYmJyZXYiOiAiSVNPIDg4Nzk6MTk4NiIsCiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAiR2xvc3NEZWYiOiB7CiAgICAgICAgICAgICAg\
ICAgICAgICAgICJwYXJhIjogIkEgbWV0YS1tYXJrdXAgbGFuZ3VhZ2UsIHVzZWQgdG8gY3JlYXRlIG1hcmt1cCBsYW5ndWFnZXMgc3VjaCBhcyBEb2NCb29rLiIsCiAgICAgICAgICAgICAgICAgIC\
AgICAgICAgICAgICAgICAgICAgICAgICAgICAgICJHbG9zc1NlZUFsc28iOiBbIkdNTCIsICJYTUwiXSwKICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgInBv\
c3RhcnJheSI6IHsKICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICJhIjoxMTEsCiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIC\
AgICAgICAgICAgICAgICAgICAgICAgICAiYiI6MjIyCiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIH0KICAgICAgICAgICAgICAgICAgICB9LAogICAgICAg\
ICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIkdsb3NzU2VlIjogIm1hcmt1cCIKICAgICAgICAgICAgICAgIH0KICAgICAgICAgICAgfQogICAgICAgIH0KICAgIH0KfQo="

#define TEST6 \
"ewoicm9vdCIgOiBbCnsKCiAgICAiZ2xvc3NhcnkiOiB7CiAgICAgICAgInRpdGxlIjogImV4YW1w\
bGUgZ2xvc3NhcnkiLAoJCSJHbG9zc0RpdiI6IHsKICAgICAgICAgICAgInRpdGxlIjogIlMiLAoJ\
CQkiR2xvc3NMaXN0IjogewogICAgICAgICAgICAgICAgIkdsb3NzRW50cnkiOiB7CiAgICAgICAg\
ICAgICAgICAgICAgIklEIjogIlNHTUwiLAoJCQkJCSJTb3J0QXMiOiAiU0dNTCIsCgkJCQkJIkds\
b3NzVGVybSI6ICJTdGFuZGFyZCBHZW5lcmFsaXplZCBNYXJrdXAgTGFuZ3VhZ2UiLAoJCQkJCSJB\
Y3JvbnltIjogIlNHTUwiLAoJCQkJCSJBYmJyZXYiOiAiSVNPIDg4Nzk6MTk4NiIsCgkJCQkJIkds\
b3NzRGVmIjogewogICAgICAgICAgICAgICAgICAgICAgICAicGFyYSI6ICJBIG1ldGEtbWFya3Vw\
IGxhbmd1YWdlLCB1c2VkIHRvIGNyZWF0ZSBtYXJrdXAgbGFuZ3VhZ2VzIHN1Y2ggYXMgRG9jQm9v\
ay4iLAoJCQkJCQkiR2xvc3NTZWVBbHNvIjogWyJHTUwiLCAiWE1MIl0sCgkJCQkJCSJwb3N0YXJy\
YXkiOiB7CgkJCQkJCQkgICJhIjoxMTEsCgkJCQkJCQkgICJiIjoyMjIKCQkJCQkJfQogICAgICAg\
ICAgICAgICAgICAgIH0sCgkJCQkJIkdsb3NzU2VlIjogIm1hcmt1cCIKICAgICAgICAgICAgICAg\
IH0sCiAgICAgICAgICAgICAgICAiR2xvc3NFbnRyeSI6IAoJCXsKICAgICAgICAgICAgICAgICAg\
ICAiSUQiOiAiU0dNTCIsCgkJCQkJIlNvcnRBcyI6ICJTR01MIiwKCQkJCQkiR2xvc3NUZXJtIjog\
IlN0YW5kYXJkIEdlbmVyYWxpemVkIE1hcmt1cCBMYW5ndWFnZSIsCgkJCQkJIkFjcm9ueW0iOiAi\
U0dNTCIsCgkJCQkJIkFiYnJldiI6ICJJU08gODg3OToxOTg2IiwKCQkJCQkiR2xvc3NEZWYiOiB7\
CiAgICAgICAgICAgICAgICAgICAgICAgICJwYXJhIjogIkEgbWV0YS1tYXJrdXAgbGFuZ3VhZ2Us\
IHVzZWQgdG8gY3JlYXRlIG1hcmt1cCBsYW5ndWFnZXMgc3VjaCBhcyBEb2NCb29rLiIsCgkJCQkJ\
CSJHbG9zc1NlZUFsc28iOiBbIkdNTCIsICJYTUwiXSwKCQkJCQkJInBvc3RhcnJheSI6IHsKCQkJ\
CQkJCSAgImEiOjExMSwKCQkJCQkJCSAgImIiOjIyMgoJCQkJCQl9CiAgICAgICAgICAgICAgICAg\
ICAgfSwKCQkJCQkiR2xvc3NTZWUiOiAibWFya3VwIgogICAgICAgICAgICAgICAgfQogICAgICAg\
ICAgICB9CiAgICAgICAgfQogICAgfQp9CiwKewoKICAgICJnbG9zc2FyeSI6IHsKICAgICAgICAi\
dGl0bGUiOiAiZXhhbXBsZSBnbG9zc2FyeSIsCgkJIkdsb3NzRGl2IjogewogICAgICAgICAgICAi\
dGl0bGUiOiAiUyIsCgkJCSJHbG9zc0xpc3QiOiB7CiAgICAgICAgICAgICAgICAiR2xvc3NFbnRy\
eSI6IHsKICAgICAgICAgICAgICAgICAgICAiSUQiOiAiU0dNTCIsCgkJCQkJIlNvcnRBcyI6ICJT\
R01MIiwKCQkJCQkiR2xvc3NUZXJtIjogIlN0YW5kYXJkIEdlbmVyYWxpemVkIE1hcmt1cCBMYW5n\
dWFnZSIsCgkJCQkJIkFjcm9ueW0iOiAiU0dNTCIsCgkJCQkJIkFiYnJldiI6ICJJU08gODg3OTox\
OTg2IiwKCQkJCQkiR2xvc3NEZWYiOiB7CiAgICAgICAgICAgICAgICAgICAgICAgICJwYXJhIjog\
IkEgbWV0YS1tYXJrdXAgbGFuZ3VhZ2UsIHVzZWQgdG8gY3JlYXRlIG1hcmt1cCBsYW5ndWFnZXMg\
c3VjaCBhcyBEb2NCb29rLiIsCgkJCQkJCSJHbG9zc1NlZUFsc28iOiBbIkdNTCIsICJYTUwiXQog\
ICAgICAgICAgICAgICAgICAgIH0sCgkJCQkJIkdsb3NzU2VlIjogIm1hcmt1cCIKICAgICAgICAg\
ICAgICAgIH0KICAgICAgICAgICAgfQogICAgICAgIH0KICAgIH0KfQpdCn0K"

std::string run_sax(const char * in)
{
	JsonParserHandler handler;
	std::string result{};
	std::function<int(JsonParserHandler::json_key_value_t&,int)> fp = [&result](JsonParserHandler::json_key_value_t& key_value,int json_idx) {
	  std::stringstream filter_result;
      filter_result.str("");
    
      std::string match_key_path;
      for(auto k : key_value.first){match_key_path.append(k);} 

		    switch(key_value.second.type()) {
			    case Valuesax::Decimal: filter_result << match_key_path << " : " << key_value.second.asInt() << "\n"; break;
			    case Valuesax::Double: filter_result << match_key_path  << " : " << key_value.second.asDouble() << "\n"; break;
			    case Valuesax::String: filter_result << match_key_path << " : " << key_value.second.asString() << "\n"; break;
			    case Valuesax::Bool: filter_result << match_key_path << " : " <<std::boolalpha << key_value.second.asBool() << "\n"; break;
			    case Valuesax::Null: filter_result << match_key_path << " : " << "null" << "\n"; break;
			    default: break;
		    }
      std::cout<<filter_result.str();
	  result += filter_result.str();
	  return 0;
    };

	//handler.key_value_criteria = true;

	handler.set_exact_match_callback( fp );
	int status = handler.process_json_buffer(base64_decode(std::string(in)).data(), strlen(in));

	if(status==0)
	{
		//return handler.get_full_result();	
	}

	return std::string("failure-sax");
}

std::string run_exact_filter(const char * in, std::vector<std::vector<std::string>>& pattern)
{
	JsonParserHandler handler;
	std::vector<std::string> keys;
	std::string result{};

	std::function<int(JsonParserHandler::json_key_value_t&,int)> fp = [&result](JsonParserHandler::json_key_value_t& key_value,int json_idx) {
	  std::stringstream filter_result;
      filter_result.str("");
      std::string match_key_path;
      for(auto k : key_value.first){match_key_path.append(k);} 
      
		    switch(key_value.second.type()) {
			    case Valuesax::Decimal: filter_result << match_key_path << " : " << key_value.second.asInt() << "\n"; break;
			    case Valuesax::Double: filter_result << match_key_path << " : " << key_value.second.asDouble() << "\n"; break;
			    case Valuesax::String: filter_result << match_key_path << " : " << key_value.second.asString() << "\n"; break;
			    case Valuesax::Bool: filter_result << match_key_path << " : " <<std::boolalpha << key_value.second.asBool() << "\n"; break;
			    case Valuesax::Null: filter_result << match_key_path << " : " << "null" << "\n"; break;
			    default: break;
		    }
      std::cout<<filter_result.str();
	  result += filter_result.str();
	  return 0;
    };

	int status{1};

	handler.set_prefix_match(pattern[0]);

	std::vector<std::vector<std::string>> pattern_minus_first(pattern.begin()+1,pattern.end());
	handler.set_exact_match_filters( pattern_minus_first );

	handler.set_exact_match_callback( fp );
	status = handler.process_json_buffer(base64_decode(std::string(in)).data(), strlen(in));

	std::cout<<"\n";

	if(!status)
	{
		return result;	
	}

	return std::string("failure-sax");
}

std::string run_dom(const char * in)
{
	rapidjson::Document document;
	document.Parse( base64_decode(std::string(in)).data() );

	if (document.HasParseError()) {
		std::cout<<"parsing error-dom"<< std::endl;
		return std::string("parsing error");
	}

	if (!document.IsObject())
	{
		std::cout << " input is not an object dom" << std::endl;
		return std::string("object error");
	}

	dom_traverse_v2 td2;
	td2.traverse( document );
	return std::string( (td2.ss).str() );
}

int compare_results(const char *in)
{
	std::cout << "===" << std::endl << base64_decode(std::string(in)) << std::endl;

	std::string dom_res = run_dom(in);
	std::string sax_res = run_sax(in);

	std::cout<<"sax res is "<<sax_res<<"\n";

	std::cout<<"dom res is "<<dom_res<<"\n";

	auto res = dom_res.compare(sax_res);

	std::cout << "dom = sax compare is :" << res << std::endl;

	return res;
}

std::string sax_exact_filter(const char *in, std::vector<std::vector<std::string>> & query_clause)
{
	std::string sax_res{};

	sax_res = run_exact_filter(in, query_clause);

	std::cout << "filter result is " << sax_res << std::endl;

	return sax_res;
}

int sax_row_count(const char *in, std::vector<std::string>& from_clause)
{
	std::string sax_res{};
	JsonParserHandler handler;
	std::vector<std::string> keys;

	std::function<int(JsonParserHandler::json_key_value_t&,int)> fp;

	int status{1};

	handler.set_prefix_match(from_clause);

	handler.set_exact_match_callback( fp );
	status = handler.process_json_buffer(base64_decode(std::string(in)).data(), strlen(in));

	std::cout<<"\n";

	if(!status)
	{
		return handler.row_count;	
	}

	return -1;
}

TEST(TestS3selectJsonParser, sax_vs_dom)
{/* the dom parser result is compared against sax parser result
	ASSERT_EQ( compare_results(TEST2) ,0); // Commenting as it is not implemented in the server side
	ASSERT_EQ( compare_results(TEST3) ,0);
	ASSERT_EQ( compare_results(TEST4) ,0);*/
}

TEST(TestS3selectJsonParser, exact_filter)
{
	std::vector<std::vector<std::string>> input = {{"row"}, {"color"}};
	std::string result_0 = R"(row/color/ : red
row/color/ : green
row/color/ : blue
row/color/ : cyan
row/color/ : magenta
row/color/ : yellow
row/color/ : black
)";
	ASSERT_EQ( sax_exact_filter(TEST2, input), result_0);

	std::vector<std::vector<std::string>> input1 = {{"nested_obj"}, {"hello2"}};
	std::string result = "nested_obj/hello2/ : world\n";
	ASSERT_EQ( sax_exact_filter(TEST3, input1), result);

	std::vector<std::vector<std::string>> input2 = {{"nested_obj"}, {"nested2", "c1"}};
	std::string result_1 = "nested_obj/nested2/c1/ : c1_value\n";
	ASSERT_EQ( sax_exact_filter(TEST3, input2), result_1);
	
	std::vector<std::vector<std::string>> input3 = {{"nested_obj"}, {"nested2", "array_nested2"}};
	std::string result_2 = R"(nested_obj/nested2/array_nested2/ : 10
nested_obj/nested2/array_nested2/ : 20
nested_obj/nested2/array_nested2/ : 30
nested_obj/nested2/array_nested2/ : 40
)";
	ASSERT_EQ( sax_exact_filter(TEST3, input3), result_2);

	std::vector<std::vector<std::string>> input4 = {{"nested_obj"}, {"nested2", "c1"}, {"nested2", "array_nested2"}};
	std::string result_3 = R"(nested_obj/nested2/c1/ : c1_value
nested_obj/nested2/array_nested2/ : 10
nested_obj/nested2/array_nested2/ : 20
nested_obj/nested2/array_nested2/ : 30
nested_obj/nested2/array_nested2/ : 40
)";
	ASSERT_EQ( sax_exact_filter(TEST3, input4), result_3);
	
	std::vector<std::vector<std::string>> input5 = {{"nested_obj", "nested3"}, {"nested4", "c1"}, {"hello3"}};
	std::string result_4 = R"(nested_obj/nested3/hello3/ : world
nested_obj/nested3/nested4/c1/ : c1_value
)";
	ASSERT_EQ( sax_exact_filter(TEST3, input5), result_4);

	std::vector<std::vector<std::string>> input6 = {{"nested_obj", "nested3"}, {"t2"}, {"nested4", "array_nested3"}};
	std::string result_5 = R"(nested_obj/nested3/t2/ : true
nested_obj/nested3/nested4/array_nested3/ : 100
nested_obj/nested3/nested4/array_nested3/ : 200
nested_obj/nested3/nested4/array_nested3/ : 300
nested_obj/nested3/nested4/array_nested3/ : 400
)";
	ASSERT_EQ( sax_exact_filter(TEST3, input6), result_5);

	std::vector<std::vector<std::string>> input7 = {{"glossary"}, {"title"}};
	std::string result_6 = "glossary/title/ : example glossary\n";
	ASSERT_EQ( sax_exact_filter(TEST4, input7), result_6);

	std::vector<std::vector<std::string>> input8 = {{"glossary"}, {"title"}, {"GlossDiv", "title"}};
	std::string result_7 = R"(glossary/title/ : example glossary
glossary/GlossDiv/title/ : S
)";
	ASSERT_EQ( sax_exact_filter(TEST4, input8), result_7);

	std::vector<std::vector<std::string>> input9 = {{"glossary", "GlossDiv"}, {"GlossList", "GlossEntry", "GlossDef", "para"}, {"GlossList", "GlossEntry", "GlossDef", "GlossSeeAlso"}};
	std::string result_8 = R"(glossary/GlossDiv/GlossList/GlossEntry/GlossDef/para/ : A meta-markup language, used to create markup languages such as DocBook.
glossary/GlossDiv/GlossList/GlossEntry/GlossDef/GlossSeeAlso/ : GML
glossary/GlossDiv/GlossList/GlossEntry/GlossDef/GlossSeeAlso/ : XML
)";
	ASSERT_EQ( sax_exact_filter(TEST4, input9), result_8);

	std::vector<std::vector<std::string>> input10 = {{"glossary", "GlossDiv"}, {"GlossList", "GlossEntry", "GlossDef", "postarray", "a"}, {"GlossList", "GlossEntry", "GlossSee"}};
	std::string result_9 = R"(glossary/GlossDiv/GlossList/GlossEntry/GlossDef/postarray/a/ : 111
glossary/GlossDiv/GlossList/GlossEntry/GlossSee/ : markup
)";
	ASSERT_EQ( sax_exact_filter(TEST5, input10), result_9);
}

TEST(TestS3selectJsonParser, iterativeParse)
{
    if(getenv("JSON_FILE"))
    {
      std::string result;
      int status = RGW_send_data(getenv("JSON_FILE"), result);
    }

}

TEST(TestS3selectJsonParser, row_count)
{
	std::vector<std::string> from_clause_0 = {"nested_obj", "nested2"};
	ASSERT_EQ( sax_row_count(TEST3, from_clause_0), 1);

	std::vector<std::string> from_clause_1 = {"nested_obj"};
	ASSERT_EQ( sax_row_count(TEST3, from_clause_1), 1);

	std::vector<std::string> from_clause_2 = {"nested_obj", "nested2", "array_nested2"};
	ASSERT_EQ( sax_row_count(TEST3, from_clause_2), 4);

	std::vector<std::string> from_clause_3 = {"nested_obj", "nested3"};
	ASSERT_EQ( sax_row_count(TEST3, from_clause_3), 1);

	std::vector<std::string> from_clause_4 = {"nested_obj", "nested3", "nested4"};
	ASSERT_EQ( sax_row_count(TEST3, from_clause_4), 1);

	std::vector<std::string> from_clause_5 = {"nested_obj", "nested3", "nested4", "array_nested3"};
	ASSERT_EQ( sax_row_count(TEST3, from_clause_5), 4);

	std::vector<std::string> from_clause_6 = {"array_1"};
	ASSERT_EQ( sax_row_count(TEST3, from_clause_6), 4);

	std::vector<std::string> from_clause_7 = {"glossary", "GlossDiv"};
	ASSERT_EQ( sax_row_count(TEST4, from_clause_7), 1);

	std::vector<std::string> from_clause_8 = {"glossary", "GlossDiv", "GlossList", "GlossEntry", "GlossDef", "GlossSeeAlso"};
	ASSERT_EQ( sax_row_count(TEST4, from_clause_8), 2);

	std::vector<std::string> from_clause_9 = {"glossary", "GlossDiv", "GlossList", "GlossEntry"};
	ASSERT_EQ( sax_row_count(TEST4, from_clause_9), 1);

	std::vector<std::string> from_clause_10 = {"glossary", "GlossDiv", "GlossList", "GlossEntry", "GlossDef"};
	ASSERT_EQ( sax_row_count(TEST4, from_clause_10), 1);

	std::vector<std::string> from_clause_11 = {"root", "glossary", "GlossDiv", "GlossList", "GlossEntry", "GlossDef", "GlossSeeAlso"};
	ASSERT_EQ( sax_row_count(TEST6, from_clause_11), 6);

	std::vector<std::string> from_clause_12 = {"root", "glossary", "GlossDiv", "GlossList", "GlossEntry", "GlossDef", "postarray"};
	ASSERT_EQ( sax_row_count(TEST6, from_clause_12), 2);

	std::vector<std::string> from_clause_13 = {"root"};
	ASSERT_EQ( sax_row_count(TEST6, from_clause_13), 2);
}

