/**
 * Pipeline merge sort
 * Lukas Dobis
 * xdobis01
 * PRL 2021
 */

#include <iostream>
#include <mpi.h>
#include <math.h>
#include <queue>
#include <fstream>

#define NUM_FILE "numbers"  /// Name of the input file with numbers.
#define FIRST_P 0 			/// Rank of the first process, reading "numbers" file.
#define PIPELINE_1_TAG 0    /// MPI tag for first input or first output process pipeline.
#define PIPELINE_2_TAG 1    /// MPI tag for second input or second output process pipeline.

//#define COMPLEXITY_TEST /// Define to measure first process start time to last process end time difference

#ifdef COMPLEXITY_TEST
#include <chrono>
#define COMPLEXITY_TEST_TAG 3 /// MPI tag to send first process start time as message to last process
#endif

/**
 * Prints an error message to standard error due to an MPI error and ends the program.
 */
void MPI_error()
{
	std::cerr << "Error: MPI library call has failed." << std::endl;
	MPI_Abort(MPI_COMM_WORLD, EXIT_FAILURE);
}

/** Pipeline Merge Sort implementation of first processor
 *
 * The first process reads numbers from the input file, prints them to the
 * standard output, and sends them to the others processes.
 */
void firstProcessor()
{	
	// Try to open file, failing to open ends program
	std::ifstream numbers(NUM_FILE);
	if (!numbers.is_open() || numbers.bad())
	{
		std::cerr << "Error: File read failed" << std::endl;
		MPI_Abort(MPI_COMM_WORLD, EXIT_FAILURE);
	}
	
	// Read, print and send numbers in MPI Send message to second process
	for (int i = 0; !numbers.eof(); ++i)
	{
		unsigned int num = numbers.get();

		if (numbers.eof())
		{
			break;
		}
		
		// Print number
		std::cout << num << " ";
		
		// Alternate between second process input pipelines
		unsigned int tag = (i % 2) == 0 ? PIPELINE_1_TAG : PIPELINE_2_TAG;
		if (MPI_Send(&num, 1, MPI_UNSIGNED_CHAR, (FIRST_P + 1), tag, MPI_COMM_WORLD))
		{
			MPI_error();
		}
	}
	
	std::cout << std::endl;
}


/**
 * Receive number on input pipeline using MPI Receive message from one rank lower process.
 *
 * @param rank Rank of a receiving process.
 * @param pipeline_1 Input pipeline 1 of a receiving process.
 * @param pipeline_2 Input pipeline 2 of a receiving process.
 */
void rec_num(const int rank, std::queue<unsigned char>& pipeline_1, std::queue<unsigned char>& pipeline_2)
{	
	MPI_Status status;
    unsigned char num;

	if (MPI_Recv(&num, 1, MPI_UNSIGNED_CHAR, (rank - 1), MPI_ANY_TAG, MPI_COMM_WORLD, &status))
	{
		MPI_error();
	}
	
    if (status.MPI_TAG == PIPELINE_1_TAG)
    {
        pipeline_1.push(num);
    }
    else if (status.MPI_TAG == PIPELINE_2_TAG)
    {
        pipeline_2.push(num);
    }
}

/**
 * Compare pipeline front numbers and return smaller number or any number on remaining pipeline when other pipeline is empty
 * or processed 2^{rank -1} numbers.
 *
 * @param pipeline_1 Input pipeline 1 of process doing comparison.
 * @param pipeline_2 Input pipeline 2 of process doing comparison.
 * @param counter_1 Counter of processed numbers from input pipeline 1.
 * @param counter_2 Counter of processed numbers from input pipeline 2.
 * @returns num Number from one of the pipelines.
 */
unsigned char pms_comp(std::queue<unsigned char>& pipeline_1, std::queue<unsigned char>& pipeline_2, unsigned int& counter_1, unsigned int& counter_2)
{	
	unsigned char num;
	
	if (counter_1 == 0)
	{
		num = pipeline_2.front();
		pipeline_2.pop();
		--counter_2;
	} else if (counter_2 == 0)
	{
		num = pipeline_1.front();
		pipeline_1.pop();
		--counter_1;
	} else if (pipeline_1.empty() && !pipeline_2.empty())
	{
		num = pipeline_1.front();
		pipeline_1.pop();
		--counter_1;
	} else if (!pipeline_1.empty() && pipeline_2.empty())
	{
		num = pipeline_2.front();
		pipeline_2.pop();
		--counter_2;
	} else if (pipeline_1.front() < pipeline_2.front())
	{	
		num = pipeline_1.front();
		pipeline_1.pop();
		--counter_1;
	} else
	{	
		num = pipeline_2.front();
		pipeline_2.pop();
		--counter_2;
	}
	
	return num;		
}

/**
 * Pipeline Merge Sort implementation of processor (except first process)
 * 
 * Each process receives numbers from previous process on his input pipelines.
 * After enough numbers on individual pipelines are received to meet process specific condition,
 * process starts sorting. Compared pairs are sent in order smaller then bigger number to input 
 * pipeline of one rank higher process. Last process does not send numbers, but prints them to
 * standard output. All processes end after processing all numbers, which count is determined 
 * from count of processes.
 *
 * @param rank Rank of a process.
 * @param procs_count Count of processes used in algorithm.
 */
void otherProcessors(const int rank, const int procs_count)
{	
	// Process input pipelines
	std::queue<unsigned char> pipeline_1;
	std::queue<unsigned char> pipeline_2;
	
	// Count of input numbers
	const int num_count = pow(2.0, procs_count - 1);
	// Count of numbers to process from single pipeline, and pipeline 1 condition to start sorting
	const int input_pipeline_cond = pow(2.0, rank - 1);
	// Number of send inputs after which process switches output pipeline through output_tag
	const int sequence_length = pow(2.0, rank);
	
	// Start comparing flag
	bool start_sort_flag = false;
	// Number of processed numbers
	unsigned int processed_num_counter = 0;
	// Initial tag of output pipeline
	unsigned int output_tag = PIPELINE_1_TAG;
	// Smaller number
	unsigned char smallNum;
	// Loop counter and counters of processed numbers on pipeline
	unsigned int i = 1;
	unsigned int counter_1 = input_pipeline_cond;
	unsigned int counter_2 = input_pipeline_cond;
	
	// Until all numbers are processed do
	while(processed_num_counter < num_count)
	{	
		++i;
		
		// Receive only if you dont have all numbers in pipelines or already had them processed
		if ((pipeline_1.size() + pipeline_2.size() + processed_num_counter) < num_count)
		{	
			rec_num(rank, pipeline_1, pipeline_2);
		}
		
		// After pipeline 1 has enough inputs and pipeline 2 has atleast 1 input, set flag to start sorting 
		if ( pipeline_1.size() >= input_pipeline_cond && pipeline_2.size() >= 1 && !start_sort_flag)
		{
			start_sort_flag = true;
		}
		
		if (start_sort_flag)
		{	
			// Get smaller number or any number if other pipeline is empty or had already processed 2^{rank -1} inputs
			smallNum = pms_comp(pipeline_1, pipeline_2, counter_1, counter_2);
			
			// Except last process, all process send number to next process
			if (rank != (procs_count - 1))
			{	
				
				if (MPI_Send(&smallNum, 1, MPI_UNSIGNED_CHAR, (rank + 1), output_tag, MPI_COMM_WORLD))
				{
					MPI_error();
				}
				
				++processed_num_counter;
				
				// After sorting sequence of length(2^{rank}) inputs switch output pipeline tag, and reset pipeline counters
				if ((processed_num_counter % sequence_length) == 0)
				{	
					output_tag = (output_tag == PIPELINE_1_TAG) ? PIPELINE_2_TAG : PIPELINE_1_TAG;
					counter_1 = input_pipeline_cond;
					counter_2 = input_pipeline_cond;
				}
				
			} else // Last process prints number to standard output 
			{	
				std::cout << (unsigned int) smallNum << std::endl;
				++processed_num_counter;
			} 
		} // sort bracket
	} // while bracket
} // func bracket

// Main 
int main(int argc, char *argv[])
{
	MPI_Init(&argc, &argv);

	int rank;
	MPI_Comm_rank(MPI_COMM_WORLD, &rank);
	
	int procs_count;
	if (MPI_Comm_size(MPI_COMM_WORLD, &procs_count))
	{	
		MPI_error();
	}
	
	// First process
	if (rank == FIRST_P)
	{	
    	#ifdef COMPLEXITY_TEST
		std::chrono::time_point<std::chrono::high_resolution_clock> start;
		start = std::chrono::high_resolution_clock::now();
		if (MPI_Send(&start, 1, MPI_DOUBLE, (procs_count - 1), COMPLEXITY_TEST_TAG, MPI_COMM_WORLD))
		{
			MPI_error();
		}
	    #endif
	    
		firstProcessor();
	}
	else 
	{	// Other processes
	 	otherProcessors(rank, procs_count);
	 	
	 	#ifdef COMPLEXITY_TEST
	    // Last process
	    if (rank == (procs_count - 1))	
	    {	
		    std::chrono::time_point<std::chrono::high_resolution_clock> start,end;
		    if (MPI_Recv(&start, 1, MPI_DOUBLE, FIRST_P, COMPLEXITY_TEST_TAG, MPI_COMM_WORLD, nullptr))
		    {
			    MPI_error();
		    }
	    
		    end = std::chrono::high_resolution_clock::now();
		    std::chrono::duration<double> diff = end - start;
		    std::cout << std::endl << "PMS time duration: " << diff.count() << "s"<< std::endl;
	    }
	    #endif
	}
	
	MPI_Finalize();

	return EXIT_SUCCESS;
}
