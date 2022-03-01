// adapted from https://gist.github.com/seungwon0/7110259

/*  garp.c - Send IPv4 Gratuitous ARP Packet

    Usage Example: sudo ./garp eth0

    Copyright (C) 2011-2013  P.D. Buchan (pdbuchan@yahoo.com)
    Copyright (C) 2013       Seungwon Jeong (seungwon0@gmail.com)

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

// Send an IPv4 Gratuitous ARP packet via raw socket at the link layer (ethernet frame).
// Values set for ARP request.

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>           // close()
#include <string.h>           // strcpy, memset(), and memcpy()

#include <sys/types.h>        // needed for socket(), uint8_t, uint16_t
#include <sys/socket.h>       // needed for socket()
#include <netinet/in.h>       // IPPROTO_RAW
#include <netinet/ip.h>       // IP_MAXPACKET (which is 65535)
#include <sys/ioctl.h>        // macro ioctl is defined
#include <bits/ioctls.h>      // defines values for argument "request" of ioctl.
#include <net/if.h>           // struct ifreq
#include <linux/if_ether.h>   // ETH_P_ARP = 0x0806
#include <linux/if_packet.h>  // struct sockaddr_ll (see man 7 packet)
#include <net/ethernet.h>

#include <errno.h>            // errno, perror()

#include "log.hpp"

// Define a struct for ARP header
typedef struct _arp_hdr arp_hdr;
struct _arp_hdr {
  uint16_t htype;
  uint16_t ptype;
  uint8_t hlen;
  uint8_t plen;
  uint16_t opcode;
  uint8_t sender_mac[6];
  uint8_t sender_ip[4];
  uint8_t target_mac[6];
  uint8_t target_ip[4];
};

// Define some constants.
#define ETH_HDRLEN 14      // Ethernet header length
#define IP4_HDRLEN 20      // IPv4 header length
#define ARP_HDRLEN 28      // ARP header length
#define ARPOP_REQUEST 1    // Taken from <linux/if_arp.h>

int
garp (uint8_t *src_ip, char *interface)
{
  int frame_length, sd, bytes;
  arp_hdr arphdr;
  uint8_t src_mac[6], dst_mac[6], ether_frame[IP_MAXPACKET];
  struct sockaddr_ll device;
  struct ifreq ifr;


  // Submit request for a socket descriptor to look up interface.
  if ((sd = socket (AF_INET, SOCK_RAW, IPPROTO_RAW)) < 0) {
    logger::log(LVL1, "socket() failed to get socket descriptor for using ioctl(): %s", strerror(errno));
    exit (EXIT_FAILURE);
  }

  // Use ioctl() to look up interface name and get its IPv4 address.
  memset (&ifr, 0, sizeof (ifr));
  snprintf (ifr.ifr_name, sizeof (ifr.ifr_name), "%s", interface);
  if (ioctl (sd, SIOCGIFADDR, &ifr) < 0) {
    logger::log(LVL1, "ioctl() failed to get source IP address: %s", strerror(errno));
    return (EXIT_FAILURE);
  }


  // Use ioctl() to look up interface name and get its MAC address.
  memset (&ifr, 0, sizeof (ifr));
  snprintf (ifr.ifr_name, sizeof (ifr.ifr_name), "%s", interface);
  if (ioctl (sd, SIOCGIFHWADDR, &ifr) < 0) {
    logger::log(LVL1, "ioctl() failed to get source MAC address: %s", strerror(errno));
    return (EXIT_FAILURE);
  }

  close (sd);

  // Copy source MAC address.
  memcpy (src_mac, ifr.ifr_hwaddr.sa_data, 6 * sizeof (uint8_t));

  // Find interface index from interface name and store index in
  // struct sockaddr_ll device, which will be used as an argument of sendto().
  if ((device.sll_ifindex = if_nametoindex (interface)) == 0) {
    logger::log(LVL1, "if_nametoindex() failed to obtain interface index: %s", strerror(errno));
    exit (EXIT_FAILURE);
  }

  // Set destination MAC address: broadcast address
  memset (dst_mac, 0xff, 6);

  memcpy (&arphdr.sender_ip, src_ip, 4);
  memcpy (&arphdr.target_ip, src_ip, 4);

  // Fill out sockaddr_ll.
  device.sll_family = AF_PACKET;
  memcpy (device.sll_addr, src_mac, 6);
  device.sll_halen = htons (6);

  // ARP header

  // Hardware type (16 bits): 1 for ethernet
  arphdr.htype = htons (1);

  // Protocol type (16 bits): 2048 for IP
  arphdr.ptype = htons (ETH_P_IP);

  // Hardware address length (8 bits): 6 bytes for MAC address
  arphdr.hlen = 6;

  // Protocol address length (8 bits): 4 bytes for IPv4 address
  arphdr.plen = 4;

  // OpCode: 1 for ARP request
  arphdr.opcode = htons (ARPOP_REQUEST);

  // Sender hardware address (48 bits): MAC address
  memcpy (&arphdr.sender_mac, src_mac, 6 * sizeof (uint8_t));

  // Target hardware address (48 bits): zero
  memset (&arphdr.target_mac, 0, 6 * sizeof (uint8_t));

  // Fill out ethernet frame header.

  // Ethernet frame length = ethernet header (MAC + MAC + ethernet type) + ethernet data (ARP header)
  frame_length = 6 + 6 + 2 + ARP_HDRLEN;

  // Destination and Source MAC addresses
  memcpy (ether_frame, dst_mac, 6 * sizeof (uint8_t));
  memcpy (ether_frame + 6, src_mac, 6 * sizeof (uint8_t));

  // Next is ethernet type code (ETH_P_ARP for ARP).
  // http://www.iana.org/assignments/ethernet-numbers
  ether_frame[12] = ETH_P_ARP / 256;
  ether_frame[13] = ETH_P_ARP % 256;

  // Next is ethernet frame data (ARP header).

  // ARP header
  memcpy (ether_frame + ETH_HDRLEN, &arphdr, ARP_HDRLEN * sizeof (uint8_t));

  // Submit request for a raw socket descriptor.
  if ((sd = socket (PF_PACKET, SOCK_RAW, htons (ETH_P_ALL))) < 0) {
    logger::log(LVL1, "socket() failed: %s", strerror(errno));
    exit (EXIT_FAILURE);
  }

  // Send ethernet frame to socket.
  if ((bytes = sendto (sd, ether_frame, frame_length, 0, (struct sockaddr *) &device, sizeof (device))) <= 0) {
    logger::log(LVL1, "sendto() failed: %s", strerror(errno));
    exit (EXIT_FAILURE);
  }

  // Close socket descriptor.
  close (sd);

  return (EXIT_SUCCESS);
}