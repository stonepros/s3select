#include "s3select.h"
#include <fstream>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <boost/crc.hpp>
#include <arpa/inet.h>
#include <boost/filesystem.hpp>
#include <boost/tokenizer.hpp>

/// implements multi-threaded execution for s3select query.
#include <boost/thread/thread.hpp>
#include <boost/lockfree/queue.hpp>
#include <boost/atomic.hpp>

using namespace s3selectEngine;
using namespace BOOST_SPIRIT_CLASSIC_NS;

int run_s3select(s3selectEngine::csv_object& s3_csv_object,
		 s3selectEngine::s3select& query_ast,
		 s3select_result& result, 
		 const char *input, size_t input_length, size_t object_size)
{
  //purpose: execution of a single stream. the result should be aggregated by the caller
  int status = 0;

  if (query_ast.get_error_description().empty() == false)
  {
    result.append(query_ast.get_error_description());
    status = -1;
  }
  else
  {
    status = s3_csv_object.run_s3select_on_stream(result, input, input_length, object_size);
    if (status < 0)
    {
      result.append(s3_csv_object.get_error_description());
    }
  }

  return status;
}

int process_on_file(char* query, char* file, shared_queue* sq)
{
  //purpose: client side "aware" of all differnt streams
  //each of the stream can execute the query independently
  
  s3selectEngine::s3select query_ast;
  csv_object::csv_defintions csv;//default
  std::ifstream input_file_stream;
  s3select_result result;//result is private per thread
  int status =0;

  //sq is shared-queue across all threads participates in execution
  result.set_shared_queue(sq);

  query_ast.parse_query(query);
  s3selectEngine::csv_object s3_csv_object(&query_ast, csv);

  //open-file
  try {
    input_file_stream = std::ifstream(file, std::ios::in | std::ios::binary);
  }
  catch( ... )
  {
    std::cout << "failed to open file " << file << std::endl;	
    return(-1);
  }

  //read-chunk
  auto file_sz = boost::filesystem::file_size(file);
#define BUFFER_SIZE (4*1024*1024)
  std::string buff(BUFFER_SIZE,0);
  while (1)
  {
    size_t read_sz = input_file_stream.readsome(buff.data(),BUFFER_SIZE);
    result.clear();

    //procesing a stream 
    status = run_s3select(s3_csv_object,
	query_ast,
	result,
	buff.data(), 
	read_sz, 
	file_sz);

    if(status<0)
    {
      std::cout << "failure on execution " << std::endl;
      break;
    }
    else 
    {
      //std::cout << "chunk complete:" << std::endl;
    }

    if(!read_sz || input_file_stream.eof())
    {
      //std::cout << "res:" << result << std::endl;
      break;
    }
  }

  return 0;
}

int run_single_query_on_many_files(char* q, std::vector<char*> files)
{
  //the set of object defines one finite data-set.
  //for CSV stream it need to know in advance the total size of data-set.

  // for(auto& f : files) { calculate total size }
  // non-aggregate query : per each object open a stream  run_s3select_on_stream(boost::lock_free::queue, ,total-size-of-all-objects) 

  shared_queue sq;
  boost::thread_group producer_threads, consumer_threads;

  //call consumer
  for(auto& f : files)
  {
      auto thread_func = [&](){return process_on_file(q,f,&sq);};
   
      producer_threads.create_thread( thread_func ); 
  }

  auto consumer_func = [&](){return sq.pop();};
  consumer_threads.create_thread(consumer_func);

  producer_threads.join_all();
  sq.producers_complete();

  consumer_threads.join_all();
 
  return 0;
}

int main(int argc, char **argv)
{
  if(argc<2) return -1;

  char* query=argv[1];
  std::vector<char*> list_of_files;

  for(int i=2;i<argc;i++)
  {	
    list_of_files.push_back(argv[i]);
  }

  run_single_query_on_many_files(query,list_of_files);

  return 0;
}

