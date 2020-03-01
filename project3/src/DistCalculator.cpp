#include "DistCalculator.hpp"
#include <chrono>
#include <cstdlib>
#include <iostream>
#include <string>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <queue>
//FROM OLD PROJ  

#include <assert.h>
#include <thread>
#include <iostream>
#include <fstream>
#include <string>
#include <cstdlib>

#include <chrono>
#include <cstdlib>
#include <iostream>
#include <string>
#include <vector>
#include <map>
#include <list>


#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <errno.h>
#include <sys/types.h>
#include <fcntl.h>
#include <unistd.h>
#include <assert.h>

#include <iostream>

//=================MULTITHREADING=====================
#include <thread>

#include <mutex>
#include <future>

//cmake -DCMAKE_BUILD_TYPE=Debug ../..
//make

#include <bits/stdc++.h> 


//With unorderedmap<unorderedset> 25/35 sec build graph

DistCalculator::DistCalculator(std::string edgeListFile)
{
	using namespace std;
	clock_t begin = clock();

   // Leave room for all nodes
	
	
	
	int handle=open(edgeListFile.c_str(),0);
	ifstream myfile (edgeListFile.c_str());
	Node actorId,movieId;
	string line;
	char useless;
	getline (myfile,line);
	int measure=0;
	int uq_Actors=0;
	int uq_movies=0;
	//cout<<"ciao"<<endl;
	while (myfile >> actorId) {
		myfile>>useless;
		myfile>>movieId;

		measure++;
		
		movies[movieId].push_back(actorId);
		actors[actorId].push_back(movieId);
		/*
   		auto gotActor = graphMap.find (actorId);
		auto gotMovie = graphMap.find (-movieId);
		if(gotActor==graphMap.end())
		{
			unordered_set<Node> tmp;
			graphMap.insert({actorId,tmp });
			uq_Actors++;
		}
		if(gotMovie==graphMap.end())
		{
			unordered_set<Node> tmp;
			graphMap.insert({-movieId,tmp });
			uq_movies++;
		}
		graphMap[-movieId].insert(actorId);
			graphMap[actorId].insert(-movieId);
			*/

		//================================TAKE TIME=========================
		/*
		if(measure%2000==0){
		clock_t end = clock();
  double elapsed_secs = double(end - begin) / CLOCKS_PER_SEC;
  	if(elapsed_secs>13)
  	{
  		throw 20;
	  cout <<elapsed_secs<<" passed"<<endl;
  	}


  }
*/

	}

	

/*
	if(graphMap.size()>200000){
		if (uq_Actors>2000000)
		{
			//throw 20;
		}
		
	}*/
	//cout<<"end build"<<endl;
}


	int64_t DistCalculator::dist( Node s,Node t) {
	// Compute a BFS tree from node s in G
		using namespace std;
		
		/*cout<<"begin"  << endl; 
		/*
		for(auto pippo:G)
		{
			cout<<pippo.first<<endl;
		}
		cout<<"end "<<endl;
		*/
		if(s==t)
		{
			return 0;
		}
		vector<int> d;
		std::vector<bool> visitedMovies;
		visitedMovies.assign(MAX_MOVIES,false);
		std::vector<bool> visitedActors;
		visitedActors.assign(MAX_ACTORS,false);
		

		visitedMovies[s]=true;

		std::vector<Node> currentLayer;; 
		currentLayer.push_back(s);

		int64_t dist=0;
		std::vector<Node> adj;
		while(!currentLayer.empty()) 
		{
			dist++;
			std::vector<Node> futureLayer;
			
			for(unsigned  i=0;i<currentLayer.size();i++)
			{
				Node currentActor=currentLayer[i];
				for(auto const& movie: actors[currentActor])
				{
					if(!visitedMovies[movie])
					{
						visitedMovies[movie]=true;
						std::vector<Node> nearbyActors=movies[movie];
						for( auto const &actorF:nearbyActors)
						{
							if(!visitedActors[actorF])
							{
								visitedActors[actorF]=true;
								futureLayer.push_back(actorF);
							}
						}
					}
				} 
			}
			currentLayer=futureLayer;

			if(visitedActors[t])
				return dist;
		
		
		
		}
return -1;
}

