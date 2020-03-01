#include <chrono>
#include <cstdlib>
#include <iostream>
#include <string>
#include <vector>
#include "DistCalculator.hpp"
#include <future>

//---------------------------------------------------------------------------
int main(int argc, char *argv[])
{
   using namespace std;
   if (argc != 2) {
      cout << "Usage: " << argv[0] << " <playedin.csv>";
      exit(1);
   }

   string playedinFile(argv[1]);
   // Create dist calculator
   DistCalculator dc(playedinFile);
   std::future<int64_t> futures[50];

   // read queries from standard in and return distances
   DistCalculator::Node a, b;
   int counter=0;
   while (cin >> a && cin >> b )
   {
      futures[counter]=std::async(std::launch::async,&DistCalculator::dist, & dc,a,b);
      //cout << dc.dist(a, b) << "\n";
      counter++;
      
   } 
   int c2=0;
   for(int i=0;i<50;i++){
        cout <<futures[i].get()<< "\n";
        c2++;
        /*if(c2>34)
         throw 20;*/
    }

   // flush output buffer
   cout.flush();
}
//---------------------------------------------------------------------------
