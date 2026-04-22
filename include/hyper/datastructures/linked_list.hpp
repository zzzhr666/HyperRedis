#pragma once
#include <cstddef>
#include <utility>
#include <concepts>

template<typename T>
struct listNode {
    listNode* next;
    listNode* prev;
    T data;
    explicit listNode(T data) : next(nullptr), prev(nullptr), data(std::move(data)) {}
    listNode(T data, listNode* next, listNode* prev) : next(next), prev(prev), data(std::move(data)) {}
};

template<typename T>
class list {
public:
    list() : len_(0), head_(nullptr), tail_(nullptr) {}

    [[nodiscard]] size_t size() const {
        return len_;
    }

    [[nodiscard]] bool empty() const {
        return len_ == 0;
    }


    list(const list&) = delete;

    list& operator=(const list&) = delete;

    list(list&& other) noexcept
        : len_(other.len_), head_(other.head_), tail_(other.tail_) {
        other.head_ = nullptr;
        other.tail_ = nullptr;
        other.len_ = 0;
    }

    list& operator=(list&& other) noexcept {
        if (this != &other) {
            clear();
            len_ = other.len_;
            head_ = other.head_;
            tail_ = other.tail_;
            other.head_ = nullptr;
            other.tail_ = nullptr;
            other.len_ = 0;
        }
        return *this;
    }


    T& front() {
        return head_->data;
    }

    const T& front() const {
        return head_->data;
    }

    T& back() {
        return tail_->data;
    }

    const T& back() const {
        return tail_->data;
    }

    void push_front(T data) {
        auto new_node = new listNode<T>(std::move(data));
        if (empty()) {
            head_ = tail_ = new_node;
        } else {
            new_node->next = head_;
            head_->prev = new_node;
            head_ = new_node;
        }
        ++len_;
    }

    void push_back(T data) {
        auto new_node = new listNode<T>(std::move(data));
        if (empty()) {
            head_ = tail_ = new_node;
        } else {
            new_node->prev = tail_;
            tail_->next = new_node;
            tail_ = new_node;
        }
        ++len_;
    }

    void pop_front() {
        if (empty()) {
            return;
        }
        if (len_ == 1) {
            delete head_;
            head_ = tail_ = nullptr;
        } else {
            auto old_head = head_;
            head_ = old_head->next;
            delete old_head;
            head_->prev = nullptr;
        }
        --len_;
    }


    void pop_back() {
        if (empty()) {
            return;
        }
        if (len_ == 1) {
            delete tail_;
            head_ = tail_ = nullptr;
        } else {
            auto old_tail = tail_;
            tail_ = old_tail->prev;
            delete old_tail;
            tail_->next = nullptr;
        }
        --len_;
    }

    const listNode<T>* find(const T& data) const {
        auto it = head_;
        while (it) {
            if (data == it->data) {
                return it;
            }
            it = it->next;
        }
        return nullptr;
    }

    listNode<T>* find(const T& data) {
        auto it = head_;
        while (it) {
            if (data == it->data) {
                return it;
            }
            it = it->next;
        }
        return nullptr;
    }

    bool contains(const T& data) const {
        return find(data) != nullptr;
    }

    void erase(listNode<T>* node) {
        if (!node || empty()) {
            return;
        }
        if (node == head_) {
            pop_front();
            return;
        }
        if (node == tail_) {
            pop_back();
            return;
        }
        node->prev->next = node->next;
        node->next->prev = node->prev;
        delete node;
        --len_;
    }

    bool erase(const T& data) {
        if (auto it = find(data)) {
            erase(it);
            return true;
        }
        return false;
    }

    void clear() {
        while (head_) {
            auto next = head_->next;
            delete head_;
            head_ = next;
        }
        head_ = tail_ = nullptr;
        len_ = 0;
    }

    ~list() {
        clear();
    }

    //helper
    template<typename Pred>
        requires std::invocable<Pred,const T&>
    void for_each(const Pred& pred) const {
        auto it = head_;
        while (it) {
            const T& data = it->data;
            pred(data);
            it = it->next;
        }
    }

    template<typename Pred>
        requires std::invocable<Pred,T&>
    void for_each(const Pred& pred) {
        auto it = head_;
        while (it) {
            pred(it->data);
            it = it->next;
        }
    }

    //iterator
    class iterator {
    public:
        explicit iterator(listNode<T>* current = nullptr) : current_(current) {}

        T* operator->() {
            return &current_->data;
        }
        T& operator*() {
            return current_->data;
        }

        iterator& operator++() {
            current_ = current_->next;
            return *this;
        }

        iterator operator++(int) {
            auto tmp = *this;
            current_ = current_->next;
            return tmp;
        }

        bool operator==(const iterator& other) const {
            return current_ == other.current_;
        }
        bool operator!=(const iterator& other) const {
            return current_ != other.current_;
        }




    private:
        listNode<T>* current_;
    };

    iterator begin(){
        return iterator{head_};
    }


    iterator end(){
        return iterator{};
    }



private:
    size_t len_;
    listNode<T>* head_;
    listNode<T>* tail_;
};
