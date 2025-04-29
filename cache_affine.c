// Manzanido, Clarence A.
// 2022-08808
// CMSC 180 CD-5L

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <math.h>
#define __USE_GNU
#include <pthread.h>
#include <sched.h>
#include <errno.h>
#include <unistd.h>
#include <sys/socket.h>
#include <arpa/inet.h>

//REMOVE for cache affinity
#define print_error_then_terminate(en, msg) \
  do { errno = en; perror(msg); exit(EXIT_FAILURE); } while (0)
#define print_perror_then_terminate(msg) \
  do { perror(msg); exit(EXIT_FAILURE); } while (0)

#define SUCCESS_MSG "Successfully set thread %lu to affinity to CPU %d\n"
#define FAILURE_MSG "Failed to set thread %lu to affinity to CPU %d\n"



typedef struct ARG{
    int start_column;
    int end_column;
    int n;
    float **submatrix;
    int submatrix_column;
    int core_id;
    int slave_port; //port
	char slave_ip[16]; //IP
}args;

void zsn(float *matrix[], int m, int start_column, int end_column){
    int sumX, sumXMinusA;
    float a,d;


    for (int j=start_column; j<end_column;j++){ // start the loop with the starting column of the thread
        // reset the summations
        sumX = 0;
        sumXMinusA = 0;

        // to get the a
        for (int s= 0; s<m; s++) { // get summation matrix[i][j]
            //printf("curr element: %f s:%d i:%d\n", matrix[s][j], s,j);
            sumX+=matrix[s][j];
           // printf("cur sum x: %d\n", sumX);
        }
      //  printf("summationX: %d i: %d\n", sumX, j);
        a =(float) sumX/m;  // divide the summation by m (m^-1 == 1/m)
      //  printf("a: %f\n", a);


        // to get the d
    //get the summation of the squared of  quantity martix minus a
        for (int k=0; k<m; k++) sumXMinusA+=(matrix[k][j]-a)*(matrix[k][j]-a);
       // printf("summationXMinusA: %d i: %d\n", sumXMinusA, j);
       //sqrt the divison of the summation and m (n^(!/2) == square root of n)
        d = (float)sqrt(sumXMinusA/m);
       // printf("d: %f\n", d);


        for (int i = 0; i < m; i++){
            matrix[i][j]=(matrix[i][j] - a)/d; //equation 1
        }
    }

    printf("done transforming the matrix\n");
}

void* thread_func(void* arg) { // thread function that takes multiple parameters
    args* data = (args*)arg;  // get the arguments
    const pthread_t pid = pthread_self(); // get the pid of running thread
    printf("coreid: %d\n", data->core_id);
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset); // initializes the CPU set to be empty set
    CPU_SET(data->core_id, &cpuset); // adds the cpu to th cpu set

    //sets the CPU affinity mask of the thread thread to the CPU set pointed to by cpuset.
    const int set_result = pthread_setaffinity_np(pid, sizeof(cpu_set_t), &cpuset);
    if (set_result != 0) {
        perror("pthread_setaffinity_np");
        pthread_exit(NULL);
    }
    //check what is the actual affinity mask that was assigned to the thread to verify the thread affinity
    //is set correctly
    const int get_affinity = pthread_getaffinity_np(pid, sizeof(cpu_set_t), &cpuset);
    if (get_affinity != 0) {
      print_error_then_terminate(get_affinity, "pthread_getaffinity_np");
    }

    char *buffer0;

    //checks if the cpu is member of the CPU set set
    if (CPU_ISSET(data->core_id, &cpuset)) {
        const size_t needed = snprintf(NULL, 0, SUCCESS_MSG, pid, data->core_id);
        buffer0 = malloc(needed);
        snprintf(buffer0, needed, SUCCESS_MSG, pid, data->core_id);
      } else {
        const size_t needed = snprintf(NULL, 0, FAILURE_MSG, pid, data->core_id);
        buffer0 = malloc(needed);
        snprintf(buffer0, needed, FAILURE_MSG, pid, data->core_id);
      }

    //printf("thread currently running\n");
    //zsn(data->submatrix, data->n, data->start_column, data->end_column); // thread will call the zsn function

    //creating a socket

    int i,j,k,l, rows, cols;
	int colCounter = 0;
	int master_socket;
	struct sockaddr_in slave_address;
    char server_message[2000], client_message[2000];


    // for checking/printing purposes
    printf("\n\nsubmatrix of thread\n");
    for(j=0; j< data->n; j++) {
        for(k=0; k<data->submatrix_column; k++) {
            printf("%f ", data->submatrix[j][k]);
        }
        printf("\n");
    }
    // Clean buffers:
    memset(server_message,'\0',sizeof(server_message));
    memset(client_message,'\0',sizeof(client_message));

	master_socket = socket(AF_INET, SOCK_STREAM, 0);
    if (master_socket == -1) {
        perror("Error: socket creation failed");
        exit(EXIT_FAILURE);
    }
    memset(&slave_address, 0, sizeof(slave_address));
    slave_address.sin_family = AF_INET;
    slave_address.sin_port = htons(data->slave_port);

    printf("%s \n", data->slave_ip);
    if (inet_pton(AF_INET, data->slave_ip, &(slave_address.sin_addr)) <= 0) {
        perror("Error: invalid slave IP address");
        exit(EXIT_FAILURE);
    }

    if (connect(master_socket, (struct sockaddr *)&slave_address, sizeof(slave_address)) < 0) {
        perror("Error: connection failed");
        exit(EXIT_FAILURE);
    }

    printf("Connected to Slave port %d\n", data->slave_port);

    //configures the number of rows and columns of the submatrix
    printf("sending row: %d\n", data->n);
    printf("sending column: %d\n", data->submatrix_column);
	rows = (data->n);
	cols = (data->submatrix_column);




	//sends the number of rows and columns to the slave
    send(master_socket, &rows, sizeof(rows), 0);
    send(master_socket, &cols, sizeof(cols), 0);

    unsigned char buffer[sizeof(float)];

    //sends the submatrix row to the slave
    for (size_t m = 0; m < rows; m++) {
    	for(size_t n = 0; n < cols; n++) {
    		memcpy(buffer, &data->submatrix[m][n], sizeof(float));
    		if (send(master_socket, buffer, sizeof(buffer), 0) == -1) {
		        perror("Send failed");
		        exit(EXIT_FAILURE);
			}
    	}
    }

	//for receiving ack
	char buffer2[1024];
	int byte_read = recv(master_socket, buffer2, 1023, 0);
    if (byte_read == -1) {
        perror("Error in receiving acknowledgement");
        exit(EXIT_FAILURE);
    }
    buffer2[byte_read] = '\0';

    if (strcmp(buffer2, "ack") != 0) {
        fprintf(stderr, "Invalid acknowledgement received");
        exit(EXIT_FAILURE);
    } else {
    	printf("ack received!\n");
    }

    close(master_socket);
    return buffer0;
}


int main(int argc, char *argv[]){
    int n,p,s,t;
    time_t t1, t2;
    float **matrix;
    FILE *file;
    int master_port;
    char line[256];
	char master_ip[16];
    printf("Enter n: ");
    scanf("%d", &n);

    printf("Enter p: ");
    scanf("%d", &p);

    printf("Enter s: ");
    scanf("%d", &s);


    // REMOVE for cache affinity
    int num_cores = sysconf(_SC_NPROCESSORS_ONLN);  // determine the number of cores of the machine
    if (num_cores < 1) {
        printf("Warning: got less than 1 number of core\n"); // warning message if less than 1 core
        num_cores = 1; //default to 1 the nmber of cores
    }
    int available_cores = num_cores - 1;   //n-1 cores to be used
    // //printf("Detected %d CPU cores\n", available_cores);

    srand(time(NULL));  // make sure random integer each run

    if (s == 0){
         // create a dynamically allocated matrix with randomized element
        matrix = (float**)malloc(sizeof(float*)*n);
        for(int i = 0; i< n; i++){
            matrix[i] = (float*)malloc(sizeof(float)*n);
            for (int j=0; j<n; j++){
            matrix[i][j] = rand() % 1000 +1;  //+1 to make sure not zero
            }
        }

        // //print initialized matrix
        printf("Initialized Matrix\n");
        for(int i = 0; i< n; i++){
            for (int j=0; j<n; j++){
                    printf("%f ", matrix[i][j]);
            }
            printf("\n");
        }

        printf("\n");

        // read config file

        file = fopen("slave_config.txt", "r");

        if (file == NULL){
            printf("Unable to open file");
            return 1;
        }
        fscanf(file, "%d\n", &t);
        // char ip[16];
        // int port;

        char ips[t][16];
        int ports[t];
        int slave_count = 0;

        while (fgets(line, sizeof(line), file) != NULL) {
	        // Separate IP address and port
	        sscanf(line, "%s %d", ips[slave_count], &ports[slave_count]);
	        printf("Read IP: %s, Port: %d\n", ips[slave_count], ports[slave_count]);
            slave_count++;


	    }

        fclose(file);

        pthread_t tid[t];
        args * arguments = (args *)malloc(sizeof(args) * (t));

        int base_cols = n / t;
        int remainder = n % t;  // this will get the number of remaining columns

        int col_start = 0;
        int col_end = 0;
        int to_be_added = 0;

        // get indices for the submatrices of matrix for threads
        for (int i = 0; i < t; i++) {
            to_be_added = 0;
            if (i < remainder){
                to_be_added = 1; // add 1 to evenly allocate the remainders
            }
            col_end = col_start + base_cols + to_be_added; //update the end column
            arguments[i].start_column = col_start;
            arguments[i].end_column = col_end;
            arguments[i].n = n;

            int submatrix_cols = col_end - col_start;

            arguments[i].submatrix = (float**)malloc(sizeof(float*) * n);
            for (int row = 0; row < n; row++) {
                arguments[i].submatrix[row] = (float*)malloc(sizeof(float) * submatrix_cols);

                // Copy the relevant portion of the original matrix
                for (int col = 0; col < submatrix_cols; col++) {
                    arguments[i].submatrix[row][col] = matrix[row][col_start + col];
                }
            }
            arguments[i].submatrix_column = submatrix_cols;
            arguments[i].core_id = i % available_cores; // assign the core id in cycle to the thread argument
            arguments[i].slave_port = ports[i];
            memcpy(arguments[i].slave_ip,ips[i], sizeof(arguments[i].slave_ip));
            col_start = col_end;  //update the starting column
        }

        time(&t1);
        for (int i = 0; i < t; i++) { // create threads
            pthread_create(&tid[i], NULL, thread_func, (void*)&arguments[i]);
        }

            // join the threads to synchronize
        for (int i = 0; i< t; i++){
            pthread_join(tid[i],NULL);
        }
        time(&t2);

        //print the transformed matrix
        // transpose_matrix_inplace(matrix, n);
        // printf("\nTransformed X:\n\n");
        // for(int i = 0; i< n; i++){
        //     for (int j=0; j<n; j++){
        //             printf("%f  ", matrix[i][j]);
        //     }
        //     printf("\n");
        // }

        // calculate the elapsed time in seconds
        double time = difftime(t2,t1);
        printf("Elapsed time: %.6f seconds\n", time);

        for(int i = 0; i< n; i++){
            free(matrix[i]);
        }
        free(matrix);

    } else if( s == 1 ) {
        int slave_socket, master_socket, addr_len;
        struct sockaddr_in slave_address, master_address;
        char server_message[2000], client_message[2000];

            // Clean buffers:
        memset(server_message, '\0', sizeof(server_message));
        memset(client_message, '\0', sizeof(client_message));
        file = fopen("master_config.txt", "r");
	    if (file == NULL) {
	        printf("Failed to open the file.\n");
	        return 1;
	    }
        if (fscanf(file, "%[^:]:%d", master_ip, &master_port) != 2) return 1;

        fscanf(file, "%[^:]:%d", master_ip, &master_port);
        printf("%s\n", master_ip);

        slave_socket = socket(AF_INET, SOCK_STREAM, 0);
        if (slave_socket < 0) {
            perror("Error in socket creation");
            exit(EXIT_FAILURE);
        }

        printf("Socket created successfully\n");

        // Set up the slave address structure
        memset(&slave_address, 0, sizeof(slave_address));
        slave_address.sin_family = AF_INET;
        slave_address.sin_addr.s_addr = INADDR_ANY;
        slave_address.sin_port = htons(p);

        // Bind the socket to the slave address
        if (bind(slave_socket, (struct sockaddr*)&slave_address, sizeof(slave_address)) < 0) {
            perror("Error in binding socket");
            exit(EXIT_FAILURE);
        }

        printf("Done with binding\n");
        // Listen for incoming connections
        if (listen(slave_socket, 1) < 0) {
            perror("Error in listening");
            exit(EXIT_FAILURE);
        }
        int master_size = sizeof(master_address);
        printf("\nListening for incoming connections.....\n");
        // Accept the master's connection
        //int master_sock_fd = accept(slave_socket,(struct sockaddr*)&master_address, (socklen_t *)&master_size);
        int master_sock_fd = accept(slave_socket,NULL,NULL);
        if (master_sock_fd < 0) {
            perror("Error in accepting connection");
            exit(EXIT_FAILURE);
        }
        printf("connection established\n");

        time(&t1);
        int rows, cols;

        //receive the number of rows and columns
        recv(master_sock_fd, &rows, sizeof(rows), 0);
        recv(master_sock_fd, &cols, sizeof(cols), 0);
        printf("rows: %d\n", rows);
        printf("cols: %d\n", cols);

        unsigned char buffer[sizeof(float)];

	    //create the submatrix
	    float **sub = (float **)malloc(rows * sizeof(float *));
	    for(size_t i=0; i < rows; i++) {
			sub[i] = (float *)malloc(sizeof(float) * (cols));
		}

		//receive each pointt and put it int the matrix
	    for (size_t j = 0; j < rows; j++) {
	        for (size_t k = 0; k < cols; k++) {
	        	if (recv(master_sock_fd, buffer, sizeof(buffer), 0) == -1) {
			    	perror("Receive failed");
			        exit(EXIT_FAILURE);
    			}
	        	memcpy(&sub[j][k], buffer, sizeof(float));
	        }
	    }

        printf("Received submatrix:\n");
        for (int i = 0; i < rows; i++) {
            printf("i: %d\n", i);
            for (int j = 0; j < cols; j++) {
                printf("%f ", sub[i][j]);
            }
            printf("\n");
        }
        printf("Processing submatrix with zsn...\n");
        zsn(sub, rows, 0, cols);  // Note: rows-1 because the actual data is rows-1

        //Print normalized submatrix
        printf("Normalized submatrix:\n");
        for (int i = 0; i < rows; i++) {
            for (int j = 0; j < cols; j++) {
                printf("%f ", sub[i][j]);
            }
            printf("\n");
        }

        // Send the processed submatrix back to master
        // printf("Sending processed submatrix back to master...\n");
        // for (int i = 0; i < rows; i++) {
        //     for (int j = 0; j < cols; j++) {
        //         memcpy(buffer, &sub[i][j], sizeof(float));
        //         if (send(master_sock_fd, buffer, sizeof(buffer), 0) == -1) {
        //             perror("Send failed");
        //             exit(EXIT_FAILURE);
        //         }
        //     }
        // }

        //for sending an ack
	    const char* ack = "ack";
	    if (send(master_sock_fd, ack, strlen(ack), 0) == -1) {
	        perror("Error in sending acknowledgement");
	        exit(EXIT_FAILURE);
	    }
        time(&t2);

        double time = difftime(t2,t1);
        printf("Elapsed time: %.6f seconds\n", time);
	    // Clean up memory
	    for (size_t i = 0; i < rows; i++) {
	    	free(sub[i]);
    	}
	    free(sub);

	    //close the sockets
        close(slave_socket);
        close(master_sock_fd);
    } else {
        printf("Invalid value of s.\n");
		return 0;
    }
    // double time = difftime(t2,t1);
    // printf("Elapsed time: %.6f seconds\n", time);
}
