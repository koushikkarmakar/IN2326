#include "JoinQuery.hpp"
#include <assert.h>
#include <fstream>
#include <thread>
#include <vector>
#include <iostream>
#include <set>
#include <atomic>
#include <thread>
#include <mutex>
#include <deque>
#include <stack>
#include <future>

using namespace std;

set<int> custKeySet;
set<int> orderKeySet;


JoinQuery::JoinQuery(string lineitem, string order, string customer)
{
    cust_path = customer;
    order_path = order;
    lineit_path = lineitem;
}

vector<string> cut(string &line, int posA, int posB)
{
    stack<int> positions;
    positions.push(posB);
    positions.push(posA);

    vector<string> res;
    string tmp;
    int counter = 0;
    for (char c : line)
    {
        if(c == '|')
        {
            if(counter == positions.top())
            {
                positions.pop();
                res.push_back(tmp);
                tmp.clear();
                if(positions.empty())
                    return res;
            } else {
                tmp.clear();
            }

            counter++;
        } else {
            tmp += c;
        }
    }
    return res;
};

set<int> partOneProcessLine(string *segmentParam, const string path, int fromLine, int toLine)
{
    string line;
    set<int> subSet;

    ifstream in;
    in.open(path);

    for (int i = 1; !in.eof(); i++)
    {
        getline(in, line);

        if (i < fromLine)
            continue;
        if (i >= toLine)
            return subSet;

        // clean line
        vector<string> res = cut(line, 0, 6);

        // filter hits
        if(res[1] != *segmentParam)
            continue;

        // Save it
        subSet.insert(stoi(res[0]));
    }
    return subSet;
}

set<int> partTwoProcessLine(const string path, int fromLine, int toLine)
{
    string line;
    set<int> subSet;

    ifstream in;
    in.open(path);

    for (int i = 1; !in.eof(); i++)
    {
        getline(in, line);

        if (i < fromLine)
            continue;
        if (i >= toLine)
            return subSet;

        // clean line
        vector<string> orders = cut(line, 0, 1);

        // filter
        if(!(custKeySet.find(stoi(orders[1])) != custKeySet.end()))    // orders::custkey not valid
            continue;

        // save
        subSet.insert(stoi(orders[0]));
    }
    return subSet;
}

pair<unsigned long int, unsigned int> partThreeProcessLine(const string path, int fromLine, int toLine)
{
    unsigned long int subSum = 0;
    unsigned int subCounter = 0;
    string line;

    ifstream in;
    in.open(path);

    for (int i = 1; !in.eof(); i++)
    {
        getline(in, line);

        if (i < fromLine)
            continue;
        if (i >= toLine)
            return make_pair(subSum, subCounter);

        // clean line
        vector<string> lineitems = cut(line, 0, 4);

        // filter
        if(!(orderKeySet.find(stoi(lineitems[0])) != orderKeySet.end()))    // orderkey not valid
            continue;

        // save
        subSum += stoi(lineitems[1]);  // sum valid quantities
        subCounter += 1;
    }
    return make_pair(subSum, subCounter);
}

size_t JoinQuery::avg(string segmentParam)
{
    static const int num_threads = 10;       // 12 hyperthreads | 6 cores

    // Part 1
    vector<future<set<int> > > subSets;

    int totalLines = (int) JoinQuery::lineCount(cust_path);
    int linesPerThread = totalLines / (num_threads + 1);        // +1 because main thread also reads a part
    int lineNum = 0;

    for (int i = 0; i < num_threads; ++i)
    {
        subSets.push_back(async(launch::async, partOneProcessLine, &segmentParam, cust_path, lineNum, (lineNum + linesPerThread)));
        lineNum += linesPerThread;
    }

    set<int> tmp1 = partOneProcessLine(&segmentParam, cust_path, lineNum, (totalLines + 1));
    custKeySet.insert(tmp1.begin(), tmp1.end());
    tmp1.clear();

    for (int i = 0; i < num_threads; ++i)
    {
        set<int> tmp = subSets[i].get();
        custKeySet.insert(tmp.begin(), tmp.end());
    }

    subSets.clear();

    // Part 2
    totalLines = (int) JoinQuery::lineCount(order_path);
    linesPerThread = totalLines / (num_threads + 1);        // +1 because main thread also reads a part
    lineNum = 0;

    for (int i = 0; i < num_threads; ++i)
    {
        subSets.push_back(async(launch::async, partTwoProcessLine, order_path, lineNum, (lineNum + linesPerThread)));
        lineNum += linesPerThread;
    }

    set<int> tmp2 = partTwoProcessLine(order_path, lineNum, (totalLines + 1));
    orderKeySet.insert(tmp2.begin(), tmp2.end());
    tmp2.clear();

    for (int i = 0; i < num_threads; ++i)
    {
        set<int> tmp = subSets[i].get();
        orderKeySet.insert(tmp.begin(), tmp.end());
    }

    custKeySet.clear();
    subSets.clear();


    // Part 3
    vector<future<std::pair<unsigned long int, unsigned int>>> subPairs;

    totalLines = (int) JoinQuery::lineCount(lineit_path);
    linesPerThread = totalLines / (num_threads + 1);        // +1 because main thread also reads a part
    lineNum = 0;

    // run threads async
    for (int i = 0; i < num_threads; ++i)
    {
        subPairs.push_back(async(launch::async, partThreeProcessLine, lineit_path, lineNum, (lineNum + linesPerThread)));
        lineNum += linesPerThread;
    }

    // run part three with main thread and collect results
    pair<unsigned long int, unsigned int> tmp3 = partThreeProcessLine(lineit_path, lineNum, (totalLines + 1));
    unsigned long int sum = tmp3.first;
    unsigned int counter = tmp3.second;

    // collect results from threads
    for (int i = 0; i < num_threads; ++i)
    {
        pair<unsigned long int, unsigned int> tmp = subPairs[i].get();
        sum += tmp.first;
        counter += tmp.second;
    }
    // clean up
    orderKeySet.clear();
    subPairs.clear();


    return (sum / (double) counter) * 100;
}

size_t JoinQuery::lineCount(std::string rel)
{
   ifstream relation(rel);
   assert(relation);  // make sure the provided string references a file
   size_t n = 0;
   for (string line; getline(relation, line);) n++;
   return n;
}

