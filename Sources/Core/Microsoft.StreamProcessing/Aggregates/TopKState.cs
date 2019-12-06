using System;
using System.Collections.Generic;
using System.Linq;

namespace Microsoft.StreamProcessing.Aggregates
{
    /// <summary>
    /// State used by TopK Aggregate
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public interface ITopKState<T>
    {
        /// <summary>
        /// Add a single entry
        /// </summary>
        /// <param name="input"></param>
        /// <param name="timestamp"></param>
        void Add(T input, long timestamp);

        /// <summary>
        /// Adds all entries from other
        /// </summary>
        /// <param name="other"></param>
        void AddAll(ITopKState<T> other);

        /// <summary>
        /// Removes the specified entry
        /// </summary>
        /// <param name="input"></param>
        /// <param name="timestamp"></param>
        void Remove(T input, long timestamp);

        /// <summary>
        /// Removes entries from other
        /// </summary>
        /// <param name="other"></param>
        void RemoveAll(ITopKState<T> other);

        /// <summary>
        /// Gets the values as sorted set
        /// </summary>
        /// <returns></returns>
        SortedMultiSet<T> GetSortedValues();

        /// <summary>
        /// Returns total number of values in the set
        /// </summary>
        long Count { get; }
    }

    internal class SimpleTopKState<T> : ITopKState<T>
    {
        private SortedMultiSet<T> values;

        public SimpleTopKState(Func<SortedDictionary<T, long>> generator)
        {
            values = new SortedMultiSet<T>(generator);
        }

        public long Count => values.TotalCount;

        public virtual void Add(T input, long timestamp)
        {
            values.Add(input);
        }

        public void AddAll(ITopKState<T> other)
        {
            values.AddAll(other.GetSortedValues());
        }

        public SortedMultiSet<T> GetSortedValues()
        {
            return values;
        }

        public void Remove(T input, long timestamp)
        {
            values.Remove(input);
        }

        public void RemoveAll(ITopKState<T> other)
        {
            values.RemoveAll(other.GetSortedValues());
        }
    }

    internal class HoppingTopKState<T> : ITopKState<T>
    {
        public long currentTimestamp;

        public SortedMultiSet<T> previousValues;
        public SortedMultiSet<T> currentValues;

        public int k;

        public Comparison<T> rankComparer;

        public HoppingTopKState(int k, Comparison<T> rankComparer, Func<SortedDictionary<T, long>> generator)
        {
            this.k = k;
            this.rankComparer = rankComparer;
            this.currentValues = new SortedMultiSet<T>(generator);
            this.previousValues = new SortedMultiSet<T>(generator);
        }

        public void Add(T input, long timestamp)
        {
            if (timestamp > currentTimestamp)
            {
                MergeCurrentToPrevious();
                currentTimestamp = timestamp;
            }
            else if (timestamp < currentTimestamp)
            {
                throw new ArgumentException("Invalid timestamp");
            }

            currentValues.Add(input);

            var toRemove = currentValues.TotalCount - k;
            if (toRemove > 0)
            {
                var min = currentValues.GetMinItem();
                if (toRemove == min.Count)
                {
                    currentValues.Remove(min.Item, min.Count);
                }
                else if (toRemove > min.Count)
                {
                    throw new InvalidOperationException("CurrentValues has more items than required");
                }
            }
        }

        public void Remove(T input, long timestamp)
        {
            if (timestamp < currentTimestamp)
            {
                previousValues.Remove(input);
            }
            else if (timestamp == currentTimestamp)
            {
                currentValues.Remove(input);
            }
            else
            {
                throw new ArgumentException("Invalid timestamp");
            }
        }

        public void RemoveAll(ITopKState<T> other)
        {
            if (other.Count != 0)
            {
                if (other is HoppingTopKState<T> otherTopK)
                {
                    if (otherTopK.currentTimestamp >= currentTimestamp)
                    {
                        throw new ArgumentException("Cannot remove entries with current or future timestamp");
                    }
                    previousValues.RemoveAll(otherTopK.currentValues);
                    previousValues.RemoveAll(otherTopK.previousValues);
                }
                else
                {
                    throw new InvalidOperationException("Cannot remove non-HoppingTopKState from HoppingTopKState");
                }
            }
        }

        public SortedMultiSet<T> GetSortedValues()
        {
            if (previousValues.IsEmpty)
                return currentValues;
            else
            {
                MergeCurrentToPrevious();
                return previousValues;
            }
        }

        private void MergeCurrentToPrevious()
        {
            if (!currentValues.IsEmpty)
            {
                // Swap so we merge small onto larger
                if (previousValues.UniqueCount < currentValues.UniqueCount)
                {
                    var temp = previousValues;
                    previousValues = currentValues;
                    currentValues = temp;
                }

                if (!currentValues.IsEmpty)
                {
                    previousValues.AddAll(currentValues);
                    currentValues.Clear();
                }
            }
        }

        public void AddAll(ITopKState<T> other)
        {
            if (other is HoppingTopKState<T> otherTopK)
            {
                if (otherTopK.currentTimestamp == currentTimestamp)
                {
                    currentValues.AddAll(otherTopK.currentValues);
                    while (currentValues.TotalCount > k)
                        currentValues.Remove(currentValues.First());
                }
                else if (otherTopK.currentTimestamp < currentTimestamp)
                {
                    previousValues.RemoveAll(otherTopK.currentValues);
                    previousValues.RemoveAll(otherTopK.previousValues);
                }
                else
                {
                    MergeCurrentToPrevious();
                    currentValues.AddAll(otherTopK.currentValues);
                    currentTimestamp = otherTopK.currentTimestamp;
                }
            }
            else
            {
                throw new InvalidOperationException("Cannot add non-HoppingTopKState from HoppingTopKState");
            }
        }

        public long Count
        {
            get => this.currentValues.TotalCount + previousValues.TotalCount;
        }
    }
}
