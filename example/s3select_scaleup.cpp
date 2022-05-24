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

class csv_streamer {

  //purpose: streamer object initiate it's own(isolated, not depended) execution flow, the caller keeps "pushing" data for processing
  private:

    s3selectEngine::s3select m_query_ast;
    std::string m_query;
    s3select_result m_result; 
    shared_queue *m_sq;//it is used for non aggregate only
    csv_object::csv_defintions csv;//default
    s3selectEngine::csv_object* m_csv_object;

  public:

    csv_streamer(std::string query, shared_queue *sq):m_query(query),m_sq(sq),m_csv_object(nullptr)
    {
      int status = m_query_ast.parse_query(m_query.data());
      
      if(status<0)
	return;//TODO stop processing

      m_csv_object = new s3selectEngine::csv_object(&m_query_ast,csv);

      m_result.set_shared_queue(nullptr);
      if(!m_query_ast.is_aggregate_query())
      {
	m_result.set_shared_queue(sq);
      }
      else
      {
	//in case of aggregation query, results saved into temporary "table"
	m_query_ast.set_execution_phase(base_statement::multiple_executions_en::FIRST_PHASE);
      }
    }
    
    s3selectEngine::s3select* getS3select()
    {
      return &m_query_ast;
    }

    ~csv_streamer()
    {
      if(m_csv_object)
	delete m_csv_object;
    }

    int process_stream(char* stream,size_t stream_size,bool end_of_stream=false)
    {
      if(end_of_stream)
      {
	m_csv_object->run_s3select_on_object(m_result, 0, 0, false, false, true);
      }
      else
      {
	m_csv_object->run_s3select_on_stream(m_result, stream, stream_size, __INT64_MAX__);
      }
      return 0;
    }

    int run_s3select(char* input,size_t input_length,size_t object_size)
    {
      //purpose: execution of a single stream. the result should be aggregated by the caller
      int status = 0;

      if (m_query_ast.get_error_description().empty() == false)
      {
	m_result.append(m_query_ast.get_error_description());
	std::cout << "syntax error:" << m_result << std::endl;
	status = -1;
      }
      else
      {
	status = m_csv_object->run_s3select_on_stream(m_result, input, input_length, object_size);
	if (status < 0)
	{
	  m_result.append(m_csv_object->get_error_description());
	  std::cout << "runtime error:" << m_result << std::endl;
	}
      }

      return status;
    }
};

int splitter()
{
  //get single object , split by size , search for \n bounderies 
  //thread per { split-data-portion --> {while(not-end-of-data-portion){read , process_stream()) }
  return 0;
}

//TODO stream_chunk()

int stream_file(char* file, csv_streamer *cs)
{//each file processed on seperate thread
  std::ifstream input_file_stream;
  int status=0;

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
#define BUFFER_SIZE (4*1024*1024)
  std::string buff(BUFFER_SIZE,0);
  while (true)
  {
    size_t read_sz = input_file_stream.readsome(buff.data(),BUFFER_SIZE);
    if(!read_sz || input_file_stream.eof())
    {//signaling end of stream
      cs->process_stream(0,0,true);
      break;
    }

    status = cs->process_stream(buff.data(),read_sz,false);
    if(status<0)
    {
      std::cout << "failure on execution " << std::endl;
      break;
    }

    if(!read_sz || input_file_stream.eof())
    {
      break;
    }
  }
  return 0;
}

int start_multiple_execution_flows(std::string q, std::vector<char*> files)
{ //the object-set defines one finite data-set for the query.

  shared_queue sq;
  boost::thread_group producer_threads, consumer_threads;
  std::vector<std::function<int(void)>> vec_of_fp;
  std::vector<csv_streamer*> all_streamers;
  std::vector<s3select*> s3select_processing_objects;

  for(auto f : files)
  {
    csv_streamer *cs = new csv_streamer(q,&sq);
    all_streamers.push_back(cs);
    auto thread_func = [f,cs](){return stream_file(f,cs);};
    vec_of_fp.push_back( thread_func );
  }

  for(auto& t : vec_of_fp)
  {
    //start with query processing
    producer_threads.create_thread( t ); 
  }

  auto consumer_func = [&](){return sq.pop();};
  //start with merging results of all threads
  consumer_threads.create_thread(consumer_func);
  producer_threads.join_all();

  //signaling.  producers complete query processing.
  sq.producers_complete();
  //upon all producers had complete their work, waiting for consumer to complete.
  consumer_threads.join_all();

  if(all_streamers[0]->getS3select()->is_aggregate_query())
  {//aggregation flow
    for(auto cs : all_streamers)
    {
      auto sa = cs->getS3select()->get_scratch_area()->get_aggregation_results();
      s3select_processing_objects.push_back(cs->getS3select());
      for(auto v : sa)
      {//debug
	std::cout << "aggregate:" << v->to_string() << std::endl;
      }
    }

    s3selectEngine::s3select main_query;
    int status = main_query.parse_query(q.data());
    if(status<0)
    {
      std::cout << "failed to parse query" << std::endl;
      return -1;
    }

    //start second phase processing(relevant only for aggregation queries)
    main_query.set_execution_phase(base_statement::multiple_executions_en::SECOND_PHASE);
    merge_results main_query_process(&main_query);
    main_query_process.set_all_processing_objects(s3select_processing_objects);
    //execution at this point, means to scan all partial results(all participants), and aggregate results reside on all AST nodes.
    main_query_process.execute_query();
    std::cout << main_query_process.get_result() << std::endl;
  }

  return 0;
}

int main(int argc, char **argv)
{
  if(argc<2) return -1;

  char* query=argv[1];
  std::string sql_query;
  sql_query.assign(query);

  std::vector<char*> list_of_files;
  setvbuf(stdout, NULL, _IONBF, 0);//unbuffered stdout

  for(int i=2;i<argc;i++)
  {	
    list_of_files.push_back(argv[i]);
  }

  start_multiple_execution_flows(sql_query, list_of_files);

  return 0;
}

