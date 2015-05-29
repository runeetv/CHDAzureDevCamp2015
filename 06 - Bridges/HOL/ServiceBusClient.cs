using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Configuration;
using Microsoft.ServiceBus;
using Microsoft.ServiceBus.Messaging;
using System.Runtime.Serialization;
using System.Runtime.Serialization.Json;

namespace ConsoleApplication2
{
    class Program
    {
        static void Main(string[] args)
        {
            string svcBusConnString = "Endpoint=sb://chdazurecamp.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=7GzdtG6wgOY/YbUlvKoWbYjX//2sVGqwfazq12msf/A=";           
            string topicName ="TestTopic";
            //CreateTopic(svcBusConnString,topicName);
            //CreateSubscriptionWihtoutFilter(svcBusConnString);
           //CreateSubscriptionWithFilter(svcBusConnString);
            //SendMessage(svcBusConnString, topicName);            
            //FetchMessages(svcBusConnString, topicName, "HighMessages");
            //SendEmployees(svcBusConnString, topicName);
            //FetchEmployees(svcBusConnString, topicName, "HRMessages");
            Console.WriteLine("Hit a key..");
            Console.ReadLine();
        }
        static void CreateTopic(string svcBusConnString,string topicName)
        {            

            var namespaceManager =
                NamespaceManager.CreateFromConnectionString(svcBusConnString);

            if (!namespaceManager.TopicExists(topicName))
            {
                namespaceManager.CreateTopic(topicName);
            }
        }
        static void CreateSubscriptionWihtoutFilter(string svcBusConnString)
        {
            
            var namespaceManager =
                NamespaceManager.CreateFromConnectionString(svcBusConnString);

            if (!namespaceManager.SubscriptionExists("TestTopic", "AllMessages"))
            {
                namespaceManager.CreateSubscription("TestTopic", "AllMessages");
            }
        }
        static void CreateSubscriptionWithFilter(string svcBusConnString)
        {
            // Create a "HighMessages" filtered subscription
            SqlFilter highMessagesFilter = new SqlFilter("MessageNumber > 3");
            var namespaceManager = NamespaceManager.CreateFromConnectionString(svcBusConnString);

            if (!namespaceManager.SubscriptionExists("TestTopic", "HighMessages"))
            { 
                namespaceManager.CreateSubscription("TestTopic",
                   "HighMessages",
                   highMessagesFilter);
            }

            // Create a "LowMessages" filtered subscription
            SqlFilter lowMessagesFilter = new SqlFilter("MessageNumber <= 3");
            if (!namespaceManager.SubscriptionExists("TestTopic", "LowMessages"))
            {
                namespaceManager.CreateSubscription("TestTopic",
               "LowMessages",
               lowMessagesFilter);
            }

            // Create a "TargetSystem" filtered subscription
            SqlFilter hrMessagesFilter = new SqlFilter("TargetSystem = 'HR'");
            if (!namespaceManager.SubscriptionExists("TestTopic", "HRMessages"))
            {
                namespaceManager.CreateSubscription("TestTopic",
               "HRMessages",
               hrMessagesFilter);
            }
        }
        static void SendMessage(string svcBusConnString, string topicName)
        {
            TopicClient Client = TopicClient.CreateFromConnectionString(svcBusConnString, topicName);

            for (int i = 0; i < 5; i++)
            {
                // Create message, passing a string message for the body
                BrokeredMessage message = new BrokeredMessage("Test message " + i);                

                // Set additional custom app-specific property
                message.Properties["MessageNumber"] = i;

                // Send message to the topic
                Client.Send(message);
            }

        }        
        static void FetchMessages(string svcBusConnString, string topicName, string subscription)
        {

            SubscriptionClient subscriptionClientHigh = SubscriptionClient.CreateFromConnectionString(svcBusConnString, topicName, subscription);

            // Configure the callback options
            OnMessageOptions options = new OnMessageOptions();
            options.AutoComplete = false;
            options.AutoRenewTimeout = TimeSpan.FromMinutes(1);

            subscriptionClientHigh.OnMessage((message) =>
            {
                try
                {
                    // Process message from subscription
                    Console.WriteLine("\n**High Messages**");
                    Console.WriteLine("Body: " + message.GetBody<string>());
                    Console.WriteLine("MessageID: " + message.MessageId);
                    Console.WriteLine("Message Number: " +
                        message.Properties["MessageNumber"]);

                    // Remove message from subscription
                    message.Complete();
                }
                catch (Exception)
                {
                    // Indicates a problem, unlock message in subscription
                    message.Abandon();
                }
            }, options);
        }
        static void SendEmployees(string svcBusConnString, string topicName)
        {
            Employee emp = new Employee();
            emp.Name = "Yash";
            emp.Deisgnation = "Dev Lead";
            emp.Dept = "SWE";

            TopicClient Client = TopicClient.CreateFromConnectionString(svcBusConnString, topicName);
            DataContractJsonSerializer ser = new DataContractJsonSerializer(typeof(Employee));
            BrokeredMessage msg = new BrokeredMessage(emp, ser);
            //msg.Properties["IsHRData"] = "1";
            msg.Properties["TargetSystem"] = "HR";
            Client.Send(msg);
        }
        static void FetchEmployees(string svcBusConnString, string topicName, string subscription)
        {

            SubscriptionClient subscriptionClientEmp =  SubscriptionClient.CreateFromConnectionString (svcBusConnString, topicName, subscription);                
            BrokeredMessage msg=  subscriptionClientEmp.Receive();
            var emp = msg.GetBody<Employee>( new DataContractJsonSerializer(typeof(Employee)));
            Console.WriteLine("\n**Emp Messages**");
            Console.WriteLine("Name: " + emp.Name);
            Console.WriteLine("Department: " + emp.Dept);
            Console.WriteLine("Designation: " + emp.Deisgnation);
            msg.Complete();
            
            
        }

    }

    public class Employee
    {
        public string Name { get; set; }
        public string Dept { get; set; }
        public string Deisgnation { get; set; }
    }
}

public void PublishWithDelay(MyEvent myEvent, DateTime utcEnqueueTime)
{
    using (var message = new BrokeredMessage(myEvent) { ScheduledEnqueueTimeUtc = utcEnqueueTime })
    {
        PublishMessage(myEvent, message);
    }
}