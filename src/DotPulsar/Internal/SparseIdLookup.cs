/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

namespace DotPulsar.Internal;

/// <summary>
/// Finds items by an id that may be any value. An id is either chosen by the caller, like the id of a producer that is the same on every connection,
/// or it is the lowest id that is not used. <see cref="IdLookup{T}"/> is the same for ids that are small indexes of an array, and it is not changed.
/// </summary>
public sealed class SparseIdLookup<T> where T : class
{
    private readonly object _lock = new();
    private readonly Dictionary<ulong, T> _items = new();

    public bool IsEmpty()
    {
        lock (_lock)
        {
            return _items.Count == 0;
        }
    }

    /// <summary>
    /// Adds the item with the lowest id that is not used, and returns the id.
    /// </summary>
    public ulong Add(T item)
    {
        lock (_lock)
        {
            ulong id = 0;

            while (_items.ContainsKey(id))
                ++id;

            _items.Add(id, item);
            return id;
        }
    }

    /// <summary>
    /// Adds the item with the id, and returns the item that had the id, or null if the id was not used.
    /// </summary>
    public T? Set(ulong id, T item)
    {
        lock (_lock)
        {
            _items.TryGetValue(id, out var replaced);
            _items[id] = item;
            return replaced;
        }
    }

    public T? Remove(ulong id)
    {
        lock (_lock)
        {
            if (!_items.TryGetValue(id, out var item))
                return null;

            _ = _items.Remove(id);
            return item;
        }
    }

    /// <summary>
    /// Removes the item if it is the one that has the id. An item that has been replaced by another one with the same id is not removed.
    /// </summary>
    public bool Remove(ulong id, T item)
    {
        lock (_lock)
        {
            if (!_items.TryGetValue(id, out var current) || !ReferenceEquals(current, item))
                return false;

            _ = _items.Remove(id);
            return true;
        }
    }

    public T[] RemoveAll()
    {
        lock (_lock)
        {
            var items = _items.Values.ToArray();
            _items.Clear();
            return items;
        }
    }

    public T? this[ulong id]
    {
        get
        {
            lock (_lock)
            {
                return _items.TryGetValue(id, out var item) ? item : null;
            }
        }
    }
}
