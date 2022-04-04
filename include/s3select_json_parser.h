#ifndef SAX_H
#define SAX_H

#include "rapidjson/reader.h"
#include "rapidjson/writer.h"
#include "rapidjson/filereadstream.h"
#include "rapidjson/filewritestream.h"
#include "rapidjson/error/en.h"
#include <cassert>
#include <sstream>
#include <fstream>
#include <vector>


class Valuesax {

  public:

    enum Type {
      Decimal,
      Double,
      String,
      Bool,
      Null
    };

  private:

    Type _type;
    double _double;
    std::string _string;
    bool _bool;
    int64_t _num;;
    std::nullptr_t _null;

  public:

    Valuesax(): _type(Decimal), _double(0.0), _bool(false),_num(0) {}

    Valuesax& Parse(std::string const& s) {
      _type = Valuesax::String;
      _string = s;
      return *this;
    }

    Valuesax& Parse(const double& s) {
      _type = Valuesax::Double;
      _double = s;
      return *this;
    }

    Valuesax& Parse(bool& s) {
      _type = Valuesax::Bool;
      _bool = s;
      return *this;
    }

    Valuesax& Parse(const int& s) {
      _type = Valuesax::Decimal;
      _num = s;
      return *this;
    }

    Valuesax& Parse(const unsigned& s) {
      _type = Valuesax::Decimal;
      _num = s;
      return *this;
    }

    Valuesax& Parse(const int64_t& s) {
      _type = Valuesax::Decimal;
      _num = s;
      return *this;
    }

    Valuesax& Parse(const uint64_t& s) {
      _type = Valuesax::Decimal;
      _num = s;
      return *this;
    }

    Valuesax& Parse(const std::nullptr_t& s) {
      _type = Valuesax::Null;
      _null = s;
      return *this;
    }


    Type type() const { return _type; }

    int asInt() const {
      assert(_type == Decimal && "not an int");
      return _num;
    }

    double asDouble() const {
      assert(_type == Double && "not a double");
      return _double;
    }

    std::string const& asString() const {
      assert(_type == String && "not a string");
      return _string;
    }

    bool asBool() const {
      assert(_type == Bool && "not a bool");
      return _bool;
    }

};

class ChunksStreamer : public rapidjson::MemoryStream {

  //purpose: adding a method `resetBuffer` that enables to parse chunk after chunk
  //per each new chunk it reset internal data members
  public:

    std::string internal_buffer;
    const Ch* next_src_;
    size_t next_size_;

    ChunksStreamer():rapidjson::MemoryStream(0,0){next_src_=0;next_size_=0;}

    ChunksStreamer(const Ch *src, size_t size) : rapidjson::MemoryStream(src,size){next_src_=0;next_size_=0;}

    Ch Peek() //const 
    {
      if(RAPIDJSON_UNLIKELY(src_ == end_))
      {
	if(next_src_)//next chunk exist
	{//upon reaching to end of current buffer, to switch with next one
	  src_ = next_src_;
	  begin_ = src_;
	  size_ =next_size_;
	  end_ = src_ + size_;

	  next_src_ = 0;
	  next_size_ = 0;
	  return *src_;
	}
	else return 0;
      }
      return *src_;
    }

    Ch Take() 
    {
      if(RAPIDJSON_UNLIKELY(src_ == end_))
      {
	if(next_src_)//next chunk exist
	{//upon reaching to end of current buffer, to switch with next one
	  src_ = next_src_;
	  begin_ = src_;
	  size_ = next_size_;
	  end_ = src_ + size_;

	  next_src_ = 0;
	  next_size_ = 0;
	  return *src_;
	}
	else return 0;
      }
      return *src_++;
    }

    void resetBuffer(char* buff, size_t size)
    {
      if(!src_)
      {//first time calling
	begin_ = buff;
	src_ = buff;
	size_ = size;
	end_= src_ + size_;
	return;
      }

      if(!next_src_)
      {//save the next-chunk that will be used upon parser reaches end of current buffer
	next_src_ = buff;
	next_size_ = size;
      }
      else
      {// should not happen
	std::cout << "can not replace pointers!!!" << std::endl;//TODO exception
	return;
      }
    }

    void saveRemainingBytes()
    {//this routine called per each new chunk
      //savine the remaining bytes, before its overriden by the next-chunk.
      size_t copy_left_sz = getBytesLeft(); //should be very small
      internal_buffer.assign(src_,copy_left_sz);
      
      src_ = internal_buffer.data();
      begin_ = src_;
      size_ = copy_left_sz;
      end_= src_ + copy_left_sz;
    }

    size_t getBytesLeft() { return end_ - src_; }

};

class MyHandler : public rapidjson::BaseReaderHandler<rapidjson::UTF8<>, MyHandler> {

  public:

  typedef enum {OBJECT_STATE,ARRAY_STATE} en_json_elm_state_t;

  public:

    std::vector < std::pair < std::string, Valuesax>> mymap;
    Valuesax value;
    ChunksStreamer stream_buffer;
    bool init_buffer_stream;
    rapidjson::Reader reader;
    std::vector<en_json_elm_state_t> json_element_state;
    std::string m_result;
    std::vector<std::string> gs_key_stack;

    MyHandler() : init_buffer_stream(false)
    {}

    std::string get_key_path()
    {//for debug
	std::string res;
	for(const auto & i: gs_key_stack)
	{
	  res.append(i);
	  res.append(std::string("/"));
	}
	return res;
    }

    void emptyhandler() {
      mymap.clear();
    }

    std::vector < std::pair < std::string, Valuesax>> get_mykeyvalue() {
      return mymap;
    }

    void dec_key_path()
    {
      if(json_element_state.back() != ARRAY_STATE)
	{
	  if(gs_key_stack.size() != 0)
	    gs_key_stack.pop_back();
	}
    }

    void push_new_key_value(Valuesax& v)//TODO should be reference
    {
      mymap.push_back(std::make_pair(get_key_path(), v));
      dec_key_path();
    }

    bool Null() {
      // at this point should verify against from-clause/where-clause/project. if match then push to scratch-area
      push_new_key_value(value.Parse(nullptr));
      return true; }

    bool Bool(bool b) {
      push_new_key_value(value.Parse(b));
      return true; }

    bool Int(int i) { 
      push_new_key_value(value.Parse(i));
      return true; }

    bool Uint(unsigned u) {
      push_new_key_value(value.Parse(u));
      return true; }

    bool Int64(int64_t i) { 
      push_new_key_value(value.Parse(i));
      return true; }

    bool Uint64(uint64_t u) { 
      push_new_key_value(value.Parse(u));
      return true; }

    bool Double(double d) { 
      push_new_key_value(value.Parse(d));
      return true; }

    bool String(const char* str, rapidjson::SizeType length, bool copy) {
      //TODO use copy
      push_new_key_value(value.Parse(str));
      return true;
    }

    bool Key(const char* str, rapidjson::SizeType length, bool copy) {
      gs_key_stack.push_back(std::string(str));
      return true;
    }

    bool StartObject() {
      json_element_state.push_back(OBJECT_STATE);
      return true; 
    }
  
    bool EndObject(rapidjson::SizeType memberCount) {
      json_element_state.pop_back();
      dec_key_path();
      return true; 
    }
 
    bool StartArray() {
      json_element_state.push_back(ARRAY_STATE);
      return true;
    }

    bool EndArray(rapidjson::SizeType elementCount) { 
      json_element_state.pop_back();
      dec_key_path();
      return true;
    }

    std::string& get_result()
    {
      return m_result;
    }

    int process_rgw_buffer(char* rgw_buffer,size_t rgw_buffer_sz, bool end_of_stream=false)
    {//RGW keeps calling with buffers, this method is not aware of object size

      std::stringstream result;

      if(!init_buffer_stream)
      {
	//set the memoryStreamer
	reader.IterativeParseInit();
	init_buffer_stream = true;
      }

      //the non-processed bytes plus the next chunk are copy into main processing buffer 
      if(!end_of_stream)
	stream_buffer.resetBuffer(rgw_buffer, rgw_buffer_sz);

      while (!reader.IterativeParseComplete()) {
	reader.IterativeParseNext<rapidjson::kParseDefaultFlags>(stream_buffer, *this);

	//IterativeParseNext returns per each parsing completion(on lexical level)
	result.str("");
	for (const auto& i : this->get_mykeyvalue()) {
	//debug purpose only 
	
	  /// pushing the key-value into s3select object. that s3seelct-object should filter according to from-clause and projection defintions
	  //  this object could remain empty (no key-value matches the search-pattern)
	  switch(i.second.type()) {
	    case Valuesax::Decimal: result << i.first << " : " << i.second.asInt() << "\n"; break;
	    case Valuesax::Double: result << i.first << " : " << i.second.asDouble() << "\n"; break;
	    case Valuesax::String: result << i.first << " : " << i.second.asString() << "\n"; break;
	    case Valuesax::Bool: result << i.first << " : " << std::boolalpha << i.second.asBool() << "\n"; break;
	    case Valuesax::Null: result << i.first << " : " << "null" << "\n"; break;
	    default: break;
	  }
	}

	//print result (actually its calling to s3select for processing. the s3slect-object may contain zero matching key-values)
	if(result.str().size())
	{
	    //std::cout << result.str();// << std::endl;
	   // m_result.append(result.str());
	}

	//once all key-values move into s3select(for further filtering and processing), it should be cleared
	this->emptyhandler();

	if (!end_of_stream && stream_buffer.next_src_==0 && stream_buffer.getBytesLeft() < 100)//TODO this condition could be replaced. it also define the amount of data that should be copy
	{//the non processed bytes will be processed on next fetched chunk
	  //TODO save remaining-bytes to internal buffer (or caller will use 2 sets of buffer)
	  stream_buffer.saveRemainingBytes();
	  return 0;
	}

	// error message
	if(reader.HasParseError())  {
	  rapidjson::ParseErrorCode c = reader.GetParseErrorCode();
	  size_t o = reader.GetErrorOffset();
	  std::cout << "PARSE ERROR " << c << " " << o << std::endl;
	  return -1;	  
	}
      }//while reader.IterativeParseComplete

      return 0;
    }

    std::string& get_full_result()
    {
      return m_result;
    }  

};

#endif

