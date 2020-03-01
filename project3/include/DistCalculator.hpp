#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#define MAX_ACTORS 2000000
#define MAX_MOVIES 1300000
using namespace std;
class DistCalculator{
public:
  using Node = uint64_t;


   vector<vector<Node>> actors= vector<vector<Node>>(MAX_ACTORS);
   vector<std::vector<Node>> movies= vector<vector<Node>>(MAX_MOVIES);
   
  DistCalculator(std::string edgeListFile);
  int64_t dist(Node a, Node b);
  
};
