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
    std::cout << "syntax error:" << result << std::endl;
    status = -1;
  }
  else
  {
    status = s3_csv_object.run_s3select_on_stream(result, input, input_length, object_size);
    if (status < 0)
    {
      result.append(s3_csv_object.get_error_description());
      std::cout << "runtime error:" << result << std::endl;
    }
  }

  return status;
}

int process_on_file(char* query, char* file,s3selectEngine::s3select* query_ast, shared_queue* sq)
{
  //purpose: client side "aware" of all differnt streams
  //each of the stream can execute the query independently
  
  csv_object::csv_defintions csv;//default
  std::ifstream input_file_stream;
  s3select_result result;//result is private per thread
  int status =0;

  //sq is shared-queue across all threads participates in execution
  result.set_shared_queue(sq);

  query_ast->parse_query(query);
  query_ast->set_execution_phase(base_statement::multiple_executions_en::FIRST_PHASE);

  s3selectEngine::csv_object s3_csv_object(query_ast, csv);

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
    if(!read_sz || input_file_stream.eof())
    {
      break;
    }

    //procesing a stream 
    std::cout << "run s3select " << read_sz << std::endl;
    status = run_s3select(s3_csv_object,
	*query_ast,
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
#if 0
      std::cout << "chunk complete:" << read_sz << std::endl;
      auto sa = query_ast->get_scratch_area()->get_aggregation_results();
      for(auto v : sa)
      {
	std::cout << "aggregate:" << v.to_string() << std::endl;
      }
#endif
    }

    if(!read_sz || input_file_stream.eof())
    {
      //std::cout << "res:" << result << std::endl;
      break;
    }
  }

  return 0;
}

int run_single_query_on_many_files_aggregate_query(char* q, std::vector<char*> files)
{ //the set of object defines one finite data-set.

  std::vector<s3selectEngine::s3select*> all_processing_object;
  std::vector<std::function<int(void)>> vec_of_fp;
  shared_queue sq;
  int status=0;
  boost::thread_group producer_threads, consumer_threads;

  s3selectEngine::s3select main_query;
  status = main_query.parse_query(q);

  if (status<0)
  {
    std::cout << "failed to parse query" << std::endl;
    return -1;
  }

  main_query.set_execution_phase(base_statement::multiple_executions_en::SECOND_PHASE);
  merge_results main_query_process(&main_query);

  for(auto f : files)
  {
    s3selectEngine::s3select * ss = new (s3selectEngine::s3select); //TODO delete
    all_processing_object.push_back(ss);
    auto thread_func = [q,f,ss,&sq](){return process_on_file(q,f,ss,&sq);};
    vec_of_fp.push_back( thread_func );
  }
 
  for(auto& t : vec_of_fp)
  {
      producer_threads.create_thread( t ); 
  }

  auto consumer_func = [&](){return sq.pop();};
  consumer_threads.create_thread(consumer_func);

  producer_threads.join_all();
  sq.producers_complete();

  consumer_threads.join_all();

  for(auto ast : all_processing_object)
  {
    auto sa = ast->get_scratch_area()->get_aggregation_results();
    for(auto v : sa)
    {//debug
      std::cout << "aggregate:" << v->to_string() << std::endl;
    }
  }

  main_query_process.set_all_processing_objects(all_processing_object);
  main_query_process.execute_query();
  std::cout << main_query_process.get_result() << std::endl;

  return 0;
}

int run_single_query_on_many_files_non_aggregate(char* q, std::vector<char*> files)
{ //the set of object defines one finite data-set. for non-aggregation flow.

  shared_queue sq;
  boost::thread_group producer_threads, consumer_threads;
  std::vector<std::function<int(void)>> vec_of_fp;

  for(auto f : files)
  {
    s3selectEngine::s3select * ss = new (s3selectEngine::s3select); //TODO delete
    auto thread_func = [q,f,ss,&sq](){return process_on_file(q,f,ss,&sq);};
    vec_of_fp.push_back( thread_func );
  }
 
  for(auto& t : vec_of_fp)
  {
      producer_threads.create_thread( t ); 
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
  setvbuf(stdout, NULL, _IONBF, 0);//unbuffered stdout

  for(int i=2;i<argc;i++)
  {	
    list_of_files.push_back(argv[i]);
  }

  //run_single_query_on_many_files_non_aggregate(query,list_of_files);
  run_single_query_on_many_files_aggregate_query(query,list_of_files);

  return 0;
}

