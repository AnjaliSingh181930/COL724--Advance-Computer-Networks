# ACNAssignment1
Bufferbloat on Network Emulator

1. Why do you see a difference in webpage fetch times with short and large router buffers?
   Ans.
   q20: Average: 0.469933
        Standard Deviation: 0.147142

   q100: Average: 1.188476
         Standard Deviation: 0.398308

   The buffer size is to blame for this. The buffer is filled by the packets sent between h1 and h2, and packet loss is not detected until the buffer is completely full. Therefore, as it takes more time to fill these buffers, times will always be longer with a larger buffer size.

   2.Bufferbloat can occur in other places such as your network interface card (NIC). Check the output of ifconfig eth0 on your VirtualBox VM. 
   What is the (maximum) transmit queue length on the network interface reported by ifconfig? For this queue size, if you assume the queue 
   drains at 100Mb/s, what is the maximum time a packet might wait in the queue before it leaves the NIC?
   Ans.
    Calling the ifconfig shows max transmit queue length ,i.e, txqueuelen=1000 and Maximum Transmission Unit ,i.e, MTU=1500. 
    Maximum time a packet might wait before it leaves nic is (1000 packets x 1500 bytes x 8 bits)/(100Mbps) = 0.12s .
   
3. How does the RTT reported by ping vary with the queue size? Describe the relation between the two.
   Ans.
   RTT and Queue_Size have a proportionate connection for bb-q20 and bbq-100 . More packets can be pushed into a larger queue, directly raising the overall RTT per Packet_Delay. **RTT$\\approx$ (queue delay + Transmission_delay + packet_delay)** .Also rtt has direct correlation with the congestion window.

   
4. Identify and describe two ways to mitigate the bufferbloat problem.
   Ans.First, by reducing the maximum buffer size, we may make the wait lighter.
  q20: Average: 0.469933
        Standard Deviation: 0.147142

  q100: Average: 1.188476
         Standard Deviation: 0.398308

  Utilizing Active Queue Management (AQM), which drops packets probabilistically to minimize problem with drop tail queues like bursty flows and global synchronization between flows, is the second way we can reduce buffer bloat.
   
9. Describe how and why your results change when you re-run the emulation.

   Ans. We could easily observe that the results have changed through the graphs that we have plotted and also the Average and Standard Deviation that we have calculated for buffer size 20 and 100.The results change because during re-run we send the data again and it might vary because of the noise ,the collision of packets that may occur, also the download time at the host will vary every time, the time taken till the packet drop occur . Due to these reasons we were seeing different results each time we re-run.

